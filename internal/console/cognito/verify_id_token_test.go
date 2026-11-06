package cognito

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

type fixtureKey struct {
	id  string
	key *rsa.PrivateKey
}

type cognitoFixture struct {
	server      *httptest.Server
	config      ProviderConfig
	roots       *x509.CertPool
	mu          sync.RWMutex
	keys        []fixtureKey
	metadata    map[string]any
	discovery   int
	badConfig   bool
	largeConfig bool
	jwksStatus  int
	largeJWKS   bool
	badJWKS     bool
	redirect    bool
	otherCalls  atomic.Int64
	jwksCalls   atomic.Int64
}

func newCognitoFixture(t *testing.T, keys ...fixtureKey) *cognitoFixture {
	t.Helper()
	fixture := &cognitoFixture{keys: keys}
	fixture.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.mu.RLock()
		defer fixture.mu.RUnlock()
		switch r.URL.Path {
		case "/pool/.well-known/openid-configuration":
			if fixture.redirect {
				http.Redirect(w, r, fixture.server.URL+"/unexpected", http.StatusFound)
				return
			}
			if fixture.discovery != 0 {
				w.WriteHeader(fixture.discovery)
				return
			}
			if fixture.badConfig {
				w.Write([]byte(`{"issuer":`))
				return
			}
			if fixture.largeConfig {
				w.Write([]byte(strings.Repeat(" ", maxProviderResponseBytes+1)))
				return
			}
			metadata := map[string]any{
				"issuer": fixture.config.Issuer, "authorization_endpoint": fixture.config.AuthorizationURL,
				"token_endpoint": fixture.config.TokenURL, "jwks_uri": fixture.config.JWKSURL,
				"id_token_signing_alg_values_supported": []string{"RS256"},
				"future_provider_field":                 true,
			}
			for key, value := range fixture.metadata {
				metadata[key] = value
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(metadata); err != nil {
				t.Error("write synthetic discovery response")
			}
		case "/pool/.well-known/jwks.json":
			fixture.jwksCalls.Add(1)
			if fixture.jwksStatus != 0 {
				w.WriteHeader(fixture.jwksStatus)
				return
			}
			if fixture.largeJWKS {
				w.Write([]byte(strings.Repeat(" ", maxProviderResponseBytes+1)))
				return
			}
			if fixture.badJWKS {
				w.Write([]byte(`{"keys":`))
				return
			}
			keys := make([]map[string]any, 0, len(fixture.keys))
			for _, entry := range fixture.keys {
				keys = append(keys, map[string]any{
					"kty": "RSA", "use": "sig", "alg": "RS256", "kid": entry.id,
					"n": base64.RawURLEncoding.EncodeToString(entry.key.N.Bytes()),
					"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(entry.key.E)).Bytes()),
				})
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{"keys": keys}); err != nil {
				t.Error("write synthetic JWKS response")
			}
		default:
			fixture.otherCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fixture.server.Close)
	origin := fixture.server.URL
	fixture.config = ProviderConfig{
		Issuer: origin + "/pool", ClientID: "synthetic-client",
		AuthorizationURL: origin + "/oauth2/authorize", TokenURL: origin + "/oauth2/token",
		JWKSURL: origin + "/pool/.well-known/jwks.json",
	}
	fixture.roots = x509.NewCertPool()
	fixture.roots.AddCert(fixture.server.Certificate())
	return fixture
}

func newFixtureKey(t *testing.T, id string) fixtureKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return fixtureKey{id: id, key: key}
}

func signFixtureToken(t *testing.T, key fixtureKey, algorithm string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": algorithm, "kid": key.id, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return message + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func cognitoFixtureClaims(config ProviderConfig, now time.Time) map[string]any {
	return map[string]any{
		"iss": config.Issuer, "sub": "opaque|subject-not-a-uuid", "aud": config.ClientID,
		"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Hour).Unix(),
		"nonce": "synthetic-nonce", "token_use": "id",
		"cognito:groups":     []string{"engineers", "admin-looking-unmapped-group"},
		"future_token_claim": map[string]any{"extra": true},
	}
}

func TestCognitoVerifyIDTokenRejectsUntrustedClaimsAndSignatures(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	key := newFixtureKey(t, "key-one")
	other := newFixtureKey(t, "key-one")
	fixture := newCognitoFixture(t, key)
	verifier, err := NewVerifier(context.Background(), fixture.config, fixture.roots, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	nonce := sha256.Sum256([]byte("synthetic-nonce"))
	valid := signFixtureToken(t, key, "RS256", cognitoFixtureClaims(fixture.config, now))
	got, err := verifier.VerifyIDToken(context.Background(), valid, nonce)
	if err != nil || got.Issuer != fixture.config.Issuer || got.Subject != "opaque|subject-not-a-uuid" ||
		!reflect.DeepEqual(got.Groups, []string{"engineers", "admin-looking-unmapped-group"}) ||
		!got.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("valid synthetic ID token was rejected: %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(map[string]any)
		key    fixtureKey
		alg    string
	}{
		{"wrong issuer", func(c map[string]any) { c["iss"] = "https://other.invalid/pool" }, key, "RS256"},
		{"wrong client", func(c map[string]any) { c["aud"] = "other-client" }, key, "RS256"},
		{"expired", func(c map[string]any) { c["exp"] = now.Add(-time.Second).Unix() }, key, "RS256"},
		{"expiry boundary", func(c map[string]any) { c["exp"] = now.Unix() }, key, "RS256"},
		{"wrong nonce", func(c map[string]any) { c["nonce"] = "wrong-nonce" }, key, "RS256"},
		{"missing nonce", func(c map[string]any) { delete(c, "nonce") }, key, "RS256"},
		{"wrong token use", func(c map[string]any) { c["token_use"] = "access" }, key, "RS256"},
		{"missing subject", func(c map[string]any) { delete(c, "sub") }, key, "RS256"},
		{"wrong group type", func(c map[string]any) { c["cognito:groups"] = "ADMIN" }, key, "RS256"},
		{"future issuance", func(c map[string]any) { c["iat"] = now.Add(6 * time.Minute).Unix() }, key, "RS256"},
		{"missing issuance", func(c map[string]any) { delete(c, "iat") }, key, "RS256"},
		{"issuance after expiry", func(c map[string]any) {
			c["iat"] = now.Add(2 * time.Minute).Unix()
			c["exp"] = now.Add(time.Minute).Unix()
		}, key, "RS256"},
		{"wrong authorized party", func(c map[string]any) { c["azp"] = "other-client" }, key, "RS256"},
		{"multiple audiences without authorized party", func(c map[string]any) { c["aud"] = []string{fixture.config.ClientID, "another-client"} }, key, "RS256"},
		{"forged signature", func(map[string]any) {}, other, "RS256"},
		{"unapproved algorithm", func(map[string]any) {}, key, "HS256"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			claims := cognitoFixtureClaims(fixture.config, now)
			tt.change(claims)
			raw := signFixtureToken(t, tt.key, tt.alg, claims)
			identity, err := verifier.VerifyIDToken(context.Background(), raw, nonce)
			if !errors.Is(err, ErrInvalidIDToken) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
				t.Fatalf("invalid token was accepted: %v", err)
			}
			if strings.Contains(err.Error(), raw) || strings.Contains(err.Error(), "synthetic-nonce") {
				t.Fatal("token verification error exposed credentials")
			}
		})
	}
	claims := cognitoFixtureClaims(fixture.config, now)
	claims["aud"] = []string{fixture.config.ClientID, "another-client"}
	claims["azp"] = fixture.config.ClientID
	delete(claims, "cognito:groups")
	identity, err := verifier.VerifyIDToken(context.Background(), signFixtureToken(t, key, "RS256", claims), nonce)
	if err != nil || len(identity.Groups) != 0 {
		t.Fatalf("valid multi-audience token without groups was rejected: %v", err)
	}
	for _, raw := range []string{"", "not-a-token", strings.Repeat("a", maxIDTokenBytes+1)} {
		identity, err := verifier.VerifyIDToken(context.Background(), raw, nonce)
		if !errors.Is(err, ErrInvalidIDToken) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
			t.Fatal("malformed ID token was accepted")
		}
	}
	if _, err := verifier.VerifyIDToken(context.Background(), valid, [32]byte{}); !errors.Is(err, ErrInvalidIDToken) {
		t.Fatal("missing nonce binding was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := verifier.VerifyIDToken(ctx, valid, nonce); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled verification was not reported: %v", err)
	}
}

func TestCognitoVerifierRequiresExactIssuerEvenForLibrarySpecialCases(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	key := newFixtureKey(t, "key-one")
	issuer := "https://accounts.google.com"
	clientID := "synthetic-client"
	verifier := &Verifier{
		verifier: oidc.NewVerifier(issuer, &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{&key.key.PublicKey}},
			&oidc.Config{ClientID: clientID, SupportedSigningAlgs: []string{"RS256"}, Now: func() time.Time { return now }}),
		issuer: issuer, clientID: clientID, now: func() time.Time { return now },
	}
	claims := cognitoFixtureClaims(ProviderConfig{Issuer: issuer, ClientID: clientID}, now)
	claims["iss"] = "accounts.google.com"
	nonce := sha256.Sum256([]byte("synthetic-nonce"))
	if _, err := verifier.VerifyIDToken(context.Background(), signFixtureToken(t, key, "RS256", claims), nonce); !errors.Is(err, ErrInvalidIDToken) {
		t.Fatalf("non-exact issuer was accepted: %v", err)
	}
}

func TestCognitoVerifierValidatesDiscoveryAndTLS(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	fixture := newCognitoFixture(t, newFixtureKey(t, "key-one"))
	clock := func() time.Time { return now }
	if _, err := NewVerifier(context.Background(), fixture.config, x509.NewCertPool(), clock); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("untrusted provider certificate was accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewVerifier(ctx, fixture.config, fixture.roots, clock); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled discovery was not reported: %v", err)
	}
	for _, tt := range []struct {
		name   string
		change func(*ProviderConfig)
	}{
		{"HTTP issuer", func(c *ProviderConfig) { c.Issuer = "http://example.test/pool" }},
		{"HTTP authorization", func(c *ProviderConfig) { c.AuthorizationURL = "http://example.test/oauth2/authorize" }},
		{"HTTP token", func(c *ProviderConfig) { c.TokenURL = "http://example.test/oauth2/token" }},
		{"HTTP JWKS", func(c *ProviderConfig) { c.JWKSURL = "http://example.test/jwks" }},
		{"userinfo", func(c *ProviderConfig) { c.Issuer = "https://user:password@example.test/pool" }},
		{"query", func(c *ProviderConfig) { c.JWKSURL += "?untrusted=1" }},
		{"fragment", func(c *ProviderConfig) { c.TokenURL += "#untrusted" }},
		{"missing client", func(c *ProviderConfig) { c.ClientID = "" }},
		{"invalid client", func(c *ProviderConfig) { c.ClientID = "bad\nclient" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := fixture.config
			tt.change(&config)
			if _, err := NewVerifier(context.Background(), config, fixture.roots, clock); !errors.Is(err, ErrInvalidProviderConfig) {
				t.Fatalf("invalid provider configuration was accepted: %v", err)
			}
		})
	}
	for _, tt := range []struct {
		name, field string
		value       any
	}{
		{"wrong issuer", "issuer", "https://other.invalid/pool"},
		{"wrong authorization", "authorization_endpoint", "https://other.invalid/authorize"},
		{"wrong token", "token_endpoint", "https://other.invalid/token"},
		{"wrong JWKS", "jwks_uri", "https://other.invalid/jwks"},
		{"missing RS256", "id_token_signing_alg_values_supported", []string{"ES256"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture.mu.Lock()
			fixture.metadata = map[string]any{tt.field: tt.value}
			fixture.mu.Unlock()
			t.Cleanup(func() {
				fixture.mu.Lock()
				fixture.metadata = nil
				fixture.mu.Unlock()
			})
			if _, err := NewVerifier(context.Background(), fixture.config, fixture.roots, clock); !errors.Is(err, ErrInvalidProvider) {
				t.Fatalf("substituted provider metadata was accepted: %v", err)
			}
		})
	}
	fixture.mu.Lock()
	fixture.redirect = true
	fixture.mu.Unlock()
	if _, err := NewVerifier(context.Background(), fixture.config, fixture.roots, clock); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("discovery redirect was accepted: %v", err)
	}
	if fixture.otherCalls.Load() != 0 {
		t.Fatal("provider discovery followed a redirect")
	}
	fixture.mu.Lock()
	fixture.redirect = false
	fixture.badConfig = true
	fixture.mu.Unlock()
	if _, err := NewVerifier(context.Background(), fixture.config, fixture.roots, clock); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("malformed discovery response was accepted: %v", err)
	}
	fixture.mu.Lock()
	fixture.badConfig = false
	fixture.largeConfig = true
	fixture.mu.Unlock()
	if _, err := NewVerifier(context.Background(), fixture.config, fixture.roots, clock); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("oversized discovery response was accepted: %v", err)
	}
}

func TestCognitoVerifierRotatesKeysAndFailsClosedOnProviderOutage(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	first, second, unknown := newFixtureKey(t, "first"), newFixtureKey(t, "second"), newFixtureKey(t, "unknown")
	fixture := newCognitoFixture(t, first)
	verifier, err := NewVerifier(context.Background(), fixture.config, fixture.roots, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	nonce := sha256.Sum256([]byte("synthetic-nonce"))
	claims := cognitoFixtureClaims(fixture.config, now)
	firstToken := signFixtureToken(t, first, "RS256", claims)
	if _, err := verifier.VerifyIDToken(context.Background(), firstToken, nonce); err != nil {
		t.Fatalf("first signing key rejected: %v", err)
	}
	fixture.mu.Lock()
	fixture.keys = []fixtureKey{second}
	fixture.mu.Unlock()
	secondToken := signFixtureToken(t, second, "RS256", claims)
	if _, err := verifier.VerifyIDToken(context.Background(), secondToken, nonce); err != nil {
		t.Fatalf("rotated signing key rejected: %v", err)
	}
	if fixture.jwksCalls.Load() != 2 {
		t.Fatalf("unexpected key refresh count=%d", fixture.jwksCalls.Load())
	}
	fixture.mu.Lock()
	fixture.jwksStatus = http.StatusServiceUnavailable
	fixture.mu.Unlock()
	if _, err := verifier.VerifyIDToken(context.Background(), secondToken, nonce); err != nil {
		t.Fatalf("cached verified key stopped working during outage: %v", err)
	}
	identity, err := verifier.VerifyIDToken(context.Background(), signFixtureToken(t, unknown, "RS256", claims), nonce)
	if !errors.Is(err, ErrProviderUnavailable) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
		t.Fatalf("provider outage was not reported: %v", err)
	}
	fixture.mu.Lock()
	fixture.jwksStatus = 0
	fixture.largeJWKS = true
	fixture.mu.Unlock()
	identity, err = verifier.VerifyIDToken(context.Background(), signFixtureToken(t, unknown, "RS256", claims), nonce)
	if !errors.Is(err, ErrProviderUnavailable) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
		t.Fatalf("oversized provider response was accepted: %v", err)
	}
	fixture.mu.Lock()
	fixture.largeJWKS = false
	fixture.badJWKS = true
	fixture.mu.Unlock()
	identity, err = verifier.VerifyIDToken(context.Background(), signFixtureToken(t, unknown, "RS256", claims), nonce)
	if !errors.Is(err, ErrProviderUnavailable) || !reflect.DeepEqual(identity, VerifiedIdentity{}) {
		t.Fatalf("malformed provider response was accepted: %v", err)
	}
}

func TestCognitoProviderResponseLimit(t *testing.T) {
	for _, tt := range []struct {
		name string
		size int
		want error
	}{
		{"exact limit", maxProviderResponseBytes, nil},
		{"over limit", maxProviderResponseBytes + 1, ErrProviderUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &providerResponseBody{
				ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("a", tt.size))),
				remaining:  maxProviderResponseBytes,
			}
			data, err := io.ReadAll(body)
			if !errors.Is(err, tt.want) || len(data) != maxProviderResponseBytes {
				t.Fatalf("size=%d error=%v", len(data), err)
			}
			if n, err := body.Read(nil); n != 0 || err != nil {
				t.Fatalf("zero-length read: size=%d error=%v", n, err)
			}
		})
	}
}
