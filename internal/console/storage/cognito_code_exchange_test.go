package storage

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

const (
	fixtureCode         = "synthetic-authorization-code"
	fixtureClientSecret = "synthetic-client-secret"
	fixtureRefresh      = "synthetic-provider-refresh-token"
	fixtureAccess       = "synthetic-provider-access-token"
)

type cognitoCodeFixture struct {
	*cognitoAttemptFixture
	server        *httptest.Server
	verifier      *cognito.Verifier
	exchanger     *cognito.CodeExchanger
	privateKey    *rsa.PrivateKey
	forgedKey     *rsa.PrivateKey
	auth          cognito.ClientAuthentication
	mode          string
	mu            sync.RWMutex
	nonce         string
	challenge     string
	tokenCalls    atomic.Int64
	redirectCalls atomic.Int64
	invalidCalls  atomic.Int64
}

func newCognitoCodeFixture(t *testing.T, mode string, auth cognito.ClientAuthentication) *cognitoCodeFixture {
	t.Helper()
	f := &cognitoCodeFixture{cognitoAttemptFixture: newCognitoAttemptFixture(t), auth: auth, mode: mode}
	var err error
	f.privateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.forgedKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/pool/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer": f.provider.Issuer, "authorization_endpoint": f.provider.AuthorizationURL,
				"token_endpoint": f.provider.TokenURL, "jwks_uri": f.provider.JWKSURL,
				"id_token_signing_alg_values_supported": []string{"RS256"},
			})
		case "/pool/.well-known/jwks.json":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "fixture",
				"n": base64.RawURLEncoding.EncodeToString(f.privateKey.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(f.privateKey.E)).Bytes()),
			}}})
		case "/oauth2/token":
			f.tokenCalls.Add(1)
			if err := r.ParseForm(); err != nil {
				f.invalidCalls.Add(1)
				http.Error(w, "bad fixture request", http.StatusBadRequest)
				return
			}
			f.mu.RLock()
			nonce, challenge, mode := f.nonce, f.challenge, f.mode
			f.mu.RUnlock()
			verifierHash := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			user, secret, basic := r.BasicAuth()
			authValid := !basic && r.Form.Get("client_id") == f.provider.ClientID && r.Form.Get("client_secret") == ""
			if f.auth == cognito.ClientAuthenticationBasic {
				authValid = basic && user == f.provider.ClientID && secret == fixtureClientSecret && r.Form.Get("client_secret") == ""
			}
			if r.Method != http.MethodPost || r.Form.Get("grant_type") != "authorization_code" ||
				r.Form.Get("code") != fixtureCode || r.Form.Get("redirect_uri") != f.callback ||
				base64.RawURLEncoding.EncodeToString(verifierHash[:]) != challenge || !authValid {
				f.invalidCalls.Add(1)
				http.Error(w, "bad fixture request", http.StatusBadRequest)
				return
			}
			switch mode {
			case "outage":
				http.Error(w, fixtureRefresh, http.StatusServiceUnavailable)
				return
			case "disconnect":
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					f.invalidCalls.Add(1)
					http.Error(w, "fixture cannot disconnect", http.StatusInternalServerError)
					return
				}
				connection, _, err := hijacker.Hijack()
				if err != nil {
					f.invalidCalls.Add(1)
					return
				}
				connection.Close()
				return
			case "redirect":
				http.Redirect(w, r, f.server.URL+"/unexpected", http.StatusFound)
				return
			case "oversized":
				w.Write([]byte(strings.Repeat(" ", (1<<20)+1)))
				return
			}
			claims := map[string]any{
				"iss": f.provider.Issuer, "sub": "opaque|synthetic-subject", "aud": f.provider.ClientID,
				"iat": f.clock().Add(-time.Minute).Unix(), "exp": f.clock().Add(time.Hour).Unix(),
				"nonce": nonce, "token_use": "id", "cognito:groups": []string{"unmapped-group"},
			}
			key := f.privateKey
			switch mode {
			case "wrong nonce":
				claims["nonce"] = "different-nonce"
			case "expired":
				claims["exp"] = f.clock().Unix()
			case "forged":
				key = f.forgedKey
			}
			idToken := signCognitoCodeFixtureToken(t, key, claims)
			payload := map[string]any{
				"token_type": "Bearer", "access_token": fixtureAccess, "id_token": idToken,
				"refresh_token": fixtureRefresh, "expires_in": 3600,
			}
			if mode == "missing refresh" {
				delete(payload, "refresh_token")
			}
			if mode == "invalid refresh" {
				payload["refresh_token"] = fixtureRefresh + "\n"
			}
			if mode == "oversized refresh" {
				payload["refresh_token"] = strings.Repeat("x", cognito.MaxRefreshTokenBytes+1)
			}
			w.Header().Set("Content-Type", "application/json")
			if mode == "exact limit" {
				data, err := json.Marshal(payload)
				if err != nil || len(data) > 1<<20 {
					f.invalidCalls.Add(1)
					http.Error(w, "bad fixture response", http.StatusInternalServerError)
					return
				}
				w.Write(data)
				w.Write([]byte(strings.Repeat(" ", (1<<20)-len(data))))
				return
			}
			_ = json.NewEncoder(w).Encode(payload)
		default:
			f.redirectCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	origin := f.server.URL
	f.provider = cognito.ProviderConfig{
		Issuer: origin + "/pool", ClientID: "test-client",
		AuthorizationURL: origin + "/oauth2/authorize", TokenURL: origin + "/oauth2/token",
		JWKSURL: origin + "/pool/.well-known/jwks.json",
	}
	var untrustedCertificate *x509.Certificate
	if mode == "untrusted token" {
		template := &x509.Certificate{
			SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &f.forgedKey.PublicKey, f.forgedKey)
		if err != nil {
			t.Fatal(err)
		}
		untrustedCertificate, err = x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		untrusted := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			f.tokenCalls.Add(1)
			http.Error(w, "unexpected untrusted request", http.StatusInternalServerError)
		}))
		untrusted.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: f.forgedKey}}}
		untrusted.StartTLS()
		t.Cleanup(untrusted.Close)
		f.provider.TokenURL = untrusted.URL + "/oauth2/token"
	}
	f.callback = "https://console.example/callback"
	f.manager, err = cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(f.server.Certificate())
	f.verifier, err = cognito.NewVerifier(context.Background(), f.provider, roots, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	if untrustedCertificate != nil {
		roots.AddCert(untrustedCertificate)
	}
	secret := ""
	if auth == cognito.ClientAuthenticationBasic {
		secret = fixtureClientSecret
	}
	f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, auth, secret)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func signCognitoCodeFixtureToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "fixture", "typ": "JWT"})
	if err != nil {
		t.Error("encode synthetic token header")
		return ""
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Error("encode synthetic token claims")
		return ""
	}
	message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Error("sign synthetic token")
		return ""
	}
	return message + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func (f *cognitoCodeFixture) consumeAttempt(t *testing.T) (cognito.AuthorizationStart, cognito.ConsumedAuthorization) {
	t.Helper()
	start, err := f.manager.StartAuthorization(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.nonce, f.challenge = start.Nonce, start.CodeChallenge
	f.mu.Unlock()
	consumed, err := f.manager.ConsumeAuthorization(context.Background(), start.State, start.BrowserBinding)
	if err != nil {
		t.Fatal(err)
	}
	return start, consumed
}

func TestCognitoCodeExchangeUsesExplicitAuthenticationAndCannotReplay(t *testing.T) {
	for _, authentication := range []cognito.ClientAuthentication{cognito.ClientAuthenticationPublic, cognito.ClientAuthenticationBasic} {
		t.Run(string(authentication), func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", authentication)
			ctx := context.Background()
			start, consumed := f.consumeAttempt(t)
			result, err := f.exchanger.ExchangeAuthorizationCode(ctx, consumed, fixtureCode)
			if err != nil || result.OperationID != start.OperationID || result.RefreshToken != fixtureRefresh ||
				result.Identity.Issuer != f.provider.Issuer || result.Identity.Subject != "opaque|synthetic-subject" ||
				!reflect.DeepEqual(result.Identity.Groups, []string{"unmapped-group"}) {
				t.Fatalf("valid synthetic code exchange failed: %v", err)
			}
			if _, err := f.exchanger.ExchangeAuthorizationCode(ctx, consumed, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("replayed code exchange was accepted: %v", err)
			}
			if f.tokenCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
				t.Fatal("code exchange was repeated or had incorrect client authentication/PKCE")
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
			if err != nil || len(events) != 3 || events[0].Outcome != AuditAttempted ||
				events[1].Outcome != AuditSucceeded || events[2].Outcome != AuditDenied {
				t.Fatalf("code exchange audit was incomplete: count=%d err=%v", len(events), err)
			}
			serialized, err := json.Marshal([]any{result, events})
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{fixtureCode, fixtureClientSecret, fixtureRefresh, fixtureAccess,
				start.State, start.Nonce, start.BrowserBinding, consumed.CodeVerifier} {
				if bytes.Contains(serialized, []byte(secret)) {
					t.Fatal("code exchange secret appeared in serialized output or audit")
				}
				for _, path := range []string{f.path, f.path + "-wal"} {
					data, err := os.ReadFile(path)
					if err != nil && !errors.Is(err, os.ErrNotExist) {
						t.Fatal(err)
					}
					if bytes.Contains(data, []byte(secret)) {
						t.Fatal("code exchange secret was persisted as plaintext")
					}
				}
			}
			f.reopen(t)
			err = f.db.ReserveCognitoCodeExchange(ctx, cognito.CodeExchangeReservation{
				OperationID: start.OperationID, DenialID: "post-restart-denial", OccurredAt: f.clock(), ExpiresAt: start.ExpiresAt,
			})
			if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || f.tokenCalls.Load() != 1 {
				t.Fatalf("exchange claim was not durable after restart: %v", err)
			}
		})
	}
}

func TestCognitoCodeExchangeAcceptsExactResponseLimit(t *testing.T) {
	f := newCognitoCodeFixture(t, "exact limit", cognito.ClientAuthenticationPublic)
	_, consumed := f.consumeAttempt(t)
	result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
	if err != nil || result.RefreshToken != fixtureRefresh || f.tokenCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
		t.Fatalf("exact response-size boundary was rejected: %v", err)
	}
}

func TestCognitoCodeExchangeProvidesBoundSessionProof(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	start, consumed := f.consumeAttempt(t)
	result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	proof, err := result.ForSession(protector, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	result.OperationID, result.Identity.Subject, result.RefreshToken = "forged-operation", "forged-subject", "forged-token"
	result.Identity.Audience[0] = "forged-audience"
	result.Identity.Groups[0] = "forged-group"
	details, err := proof.Details(protector, f.clock())
	if err != nil || details.OperationID != start.OperationID || details.Identity.Subject != "opaque|synthetic-subject" ||
		details.Identity.Audience[0] != f.provider.ClientID || details.Identity.Groups[0] != "unmapped-group" ||
		details.RefreshToken != fixtureRefresh || details.NonceHash != consumed.NonceHash ||
		!details.ClaimsFreshUntil.Equal(time.Unix(f.clock().Add(-time.Minute).Unix(), 0).Add(5*time.Minute)) {
		t.Fatalf("session proof was affected by mutable exchange fields: %v", err)
	}
	details.Identity.Audience[0] = "mutated-copy"
	details, err = proof.Details(protector, f.clock())
	if err != nil || details.Identity.Audience[0] != f.provider.ClientID {
		t.Fatalf("session proof returned a mutable identity alias: %v", err)
	}
	serialized, err := json.Marshal(details)
	if err != nil || bytes.Contains(serialized, []byte(fixtureRefresh)) || bytes.Contains(serialized, []byte("opaque|synthetic-subject")) {
		t.Fatal("session exchange details exposed provider credentials or attributes in JSON")
	}
	if _, err := (cognito.VerifiedCodeExchange{}).Details(protector, f.clock()); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("forged exchange proof was accepted: %v", err)
	}
	if _, err := result.ForSession(nil, f.clock()); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("missing protector was accepted: %v", err)
	}
	otherManager, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, "https://console.example/other")
	if err != nil {
		t.Fatal(err)
	}
	otherProtector, err := cognito.NewRefreshTokenProtector(otherManager, rand.Reader, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proof.Details(otherProtector, f.clock()); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("session proof accepted another provider binding: %v", err)
	}
	if _, err := proof.Details(protector, details.ValidUntil); !errors.Is(err, cognito.ErrCodeExchangeExpired) {
		t.Fatalf("expired session proof was accepted: %v", err)
	}
	if _, err := proof.Details(protector, f.clock().Add(-time.Second)); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("session proof accepted a clock earlier than verification: %v", err)
	}
}

func TestCognitoCodeExchangeRejectsInvalidTokensAndUnknownOutcomes(t *testing.T) {
	for _, tt := range []struct {
		mode string
		want error
	}{
		{"wrong nonce", cognito.ErrInvalidIDToken},
		{"expired", cognito.ErrInvalidIDToken},
		{"forged", cognito.ErrInvalidIDToken},
		{"missing refresh", cognito.ErrCodeExchangeUnknown},
		{"invalid refresh", cognito.ErrCodeExchangeUnknown},
		{"oversized refresh", cognito.ErrCodeExchangeUnknown},
		{"outage", cognito.ErrCodeExchangeUnknown},
		{"disconnect", cognito.ErrCodeExchangeUnknown},
		{"redirect", cognito.ErrCodeExchangeUnknown},
		{"oversized", cognito.ErrCodeExchangeUnknown},
		{"untrusted token", cognito.ErrCodeExchangeUnknown},
	} {
		t.Run(tt.mode, func(t *testing.T) {
			f := newCognitoCodeFixture(t, tt.mode, cognito.ClientAuthenticationPublic)
			_, consumed := f.consumeAttempt(t)
			result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
			if !errors.Is(err, tt.want) || result.RefreshToken != "" || !reflect.DeepEqual(result.Identity, cognito.VerifiedIdentity{}) {
				t.Fatalf("invalid or uncertain exchange returned credentials: %v", err)
			}
			if strings.Contains(err.Error(), fixtureCode) || strings.Contains(err.Error(), fixtureRefresh) {
				t.Fatal("exchange error exposed provider credentials")
			}
			if _, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
				t.Fatalf("failed exchange was replayed: %v", err)
			}
			wantCalls := int64(1)
			if tt.mode == "untrusted token" {
				wantCalls = 0
			}
			if f.tokenCalls.Load() != wantCalls || f.redirectCalls.Load() != 0 || f.invalidCalls.Load() != 0 {
				t.Fatal("failed exchange retried or followed a redirect")
			}
			events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
			want := AuditFailed
			if errors.Is(tt.want, cognito.ErrCodeExchangeUnknown) {
				want = AuditUnknown
			}
			if err != nil || len(events) != 3 || events[0].Outcome != AuditAttempted || events[1].Outcome != want || events[2].Outcome != AuditDenied {
				t.Fatalf("failed exchange outcome was not audited: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestCognitoCodeExchangeDoesNotProbeClientAuthenticationOnFailure(t *testing.T) {
	f := newCognitoCodeFixture(t, "outage", cognito.ClientAuthenticationBasic)
	_, consumed := f.consumeAttempt(t)
	if _, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnknown) {
		t.Fatalf("provider outage was not reported: %v", err)
	}
	if f.tokenCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
		t.Fatal("confidential-client failure triggered an authentication-style retry")
	}
}

func TestCognitoCodeExchangeBindsProviderCallbackAndConsumedProof(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, consumed := f.consumeAttempt(t)
	if _, err := cognito.NewCodeExchanger(f.manager, f.verifier, "https://console.example/other", cognito.ClientAuthenticationPublic, ""); !errors.Is(err, cognito.ErrCodeExchangeConfig) {
		t.Fatalf("changed callback accepted: %v", err)
	}
	if _, err := cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, fixtureClientSecret); !errors.Is(err, cognito.ErrCodeExchangeConfig) {
		t.Fatalf("public client with secret accepted: %v", err)
	}
	if _, err := cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationBasic, ""); !errors.Is(err, cognito.ErrCodeExchangeConfig) {
		t.Fatalf("confidential client without secret accepted: %v", err)
	}
	if _, err := cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthentication("auto"), ""); !errors.Is(err, cognito.ErrCodeExchangeConfig) {
		t.Fatalf("automatic client-authentication probing accepted: %v", err)
	}
	forged := cognito.ConsumedAuthorization{OperationID: consumed.OperationID, NonceHash: consumed.NonceHash, CodeVerifier: consumed.CodeVerifier}
	if _, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), forged, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("unconsumed proof was accepted: %v", err)
	}
	consumed.OperationID, consumed.NonceHash, consumed.CodeVerifier = "modified", [32]byte{}, "modified"
	result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
	if err != nil || result.OperationID == "modified" || f.invalidCalls.Load() != 0 {
		t.Fatalf("mutable caller fields replaced the consumed proof: %v", err)
	}
}

func TestCognitoCodeExchangeSerializesConcurrentRequests(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationBasic)
	_, consumed := f.consumeAttempt(t)
	const callers = 16
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	success, denied := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, cognito.ErrCodeExchangeUnavailable):
			denied++
		default:
			t.Fatalf("unexpected concurrent exchange error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.tokenCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
		t.Fatalf("exchange was not single-use: success=%d denied=%d calls=%d", success, denied, f.tokenCalls.Load())
	}
}

func TestCognitoCodeExchangeAuditFailureBlocksOrWithholdsTokens(t *testing.T) {
	for _, outcome := range []string{"attempted", "succeeded"} {
		t.Run(outcome, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			_, consumed := f.consumeAttempt(t)
			_, err := f.db.db.ExecContext(context.Background(), `CREATE TRIGGER reject_code_exchange_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.exchange' AND NEW.outcome = '`+outcome+`'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
			if !errors.Is(err, cognito.ErrCodeExchangeAudit) || result.RefreshToken != "" || !reflect.DeepEqual(result.Identity, cognito.VerifiedIdentity{}) {
				t.Fatalf("audit failure returned exchange credentials: %v", err)
			}
			wantCalls := int64(0)
			if outcome == "succeeded" {
				wantCalls = 1
				if _, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
					t.Fatalf("unaudited outcome was replayed: %v", err)
				}
			}
			if f.tokenCalls.Load() != wantCalls {
				t.Fatalf("provider calls after audit failure=%d", f.tokenCalls.Load())
			}
		})
	}
}

func TestCognitoCodeExchangeUnknownReservationDoesNotContactProvider(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, consumed := f.consumeAttempt(t)
	_, err := f.db.db.ExecContext(context.Background(), `CREATE TABLE code_exchange_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_code_exchange_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.exchange' AND NEW.outcome = 'attempted'
BEGIN INSERT INTO code_exchange_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
	if !errors.Is(err, cognito.ErrCodeExchangeUnknown) || result.RefreshToken != "" || f.tokenCalls.Load() != 0 {
		t.Fatalf("unknown reservation contacted provider or returned credentials: %v", err)
	}
}

func TestCognitoCodeExchangeRechecksExpiryAfterQueueing(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, consumed := f.consumeAttempt(t)
	initial := f.clock()
	err := runQueuedDatabaseOperation(t, f.db, func() {
		f.current.Store(initial.Add(cognito.AttemptLifetime).UnixNano())
	}, func() error {
		_, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode)
		return err
	})
	if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || f.tokenCalls.Load() != 0 {
		t.Fatalf("queued expired exchange contacted provider: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied ||
		!events[0].OccurredAt.Equal(initial.Add(cognito.AttemptLifetime)) {
		t.Fatalf("expired exchange denial was not audited: count=%d err=%v", len(events), err)
	}
}

func TestCognitoCodeExchangeRechecksResultExpiryAfterQueueing(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	start, _ := f.consumeAttempt(t)
	initial := f.clock()
	err := f.db.ReserveCognitoCodeExchange(context.Background(), cognito.CodeExchangeReservation{
		OperationID: start.OperationID, DenialID: "unused-denial", OccurredAt: initial, ExpiresAt: start.ExpiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = runQueuedDatabaseOperation(t, f.db, func() {
		f.current.Store(start.ExpiresAt.UnixNano())
	}, func() error {
		return f.db.FinishCognitoCodeExchange(context.Background(), cognito.CodeExchangeOutcome{
			OperationID: start.OperationID, OccurredAt: initial, ValidUntil: start.ExpiresAt, Status: cognito.CodeExchangeSucceeded,
		})
	})
	if !errors.Is(err, cognito.ErrCodeExchangeExpired) {
		t.Fatalf("expired exchange outcome was accepted: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditValidationRejected ||
		!events[1].OccurredAt.Equal(start.ExpiresAt) {
		t.Fatalf("expired exchange outcome was not audited: count=%d err=%v", len(events), err)
	}
}

func TestCognitoCodeExchangeInvalidCodeIsConsumedWithoutProviderRequest(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	_, consumed := f.consumeAttempt(t)
	result, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, "invalid\ncode")
	if !errors.Is(err, cognito.ErrCodeExchangeUnavailable) || result.RefreshToken != "" || f.tokenCalls.Load() != 0 {
		t.Fatalf("malformed code was sent: %v", err)
	}
	if _, err := f.exchanger.ExchangeAuthorizationCode(context.Background(), consumed, fixtureCode); !errors.Is(err, cognito.ErrCodeExchangeUnavailable) {
		t.Fatalf("malformed code exchange was retried: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.exchange", Limit: 10})
	if err != nil || len(events) != 3 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditFailed || events[2].Outcome != AuditDenied {
		t.Fatalf("malformed code decisions not audited: %s", fmt.Sprint(err))
	}
}
