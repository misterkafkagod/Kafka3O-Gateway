package main

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	syntheticCognitoClientID = "synthetic-client"
	syntheticCognitoSecret   = "synthetic-client-secret-value"
)

var (
	syntheticAttemptKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x31}, 32))
	syntheticRefreshKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x52}, 32))
)

// syntheticCognitoProvider is a verified-HTTPS Cognito-compatible discovery, JWKS and token endpoint.
type syntheticCognitoProvider struct {
	server          *httptest.Server
	caFile          string
	key             *rsa.PrivateKey
	discoveryStatus atomic.Int32
	issuerMismatch  atomic.Bool
	tokenStatus     atomic.Int32
	wrongNonce      atomic.Bool
	tokenCalls      atomic.Int64
	mu              sync.Mutex
	codes           map[string]syntheticGrant
	refreshTokens   map[string]syntheticGrant
}

type syntheticGrant struct {
	nonce, challenge, redirect string
	groups                     []string
}

func newSyntheticCognitoProvider(t *testing.T) *syntheticCognitoProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	provider := &syntheticCognitoProvider{key: key, codes: map[string]syntheticGrant{}, refreshTokens: map[string]syntheticGrant{}}
	provider.server = httptest.NewTLSServer(http.HandlerFunc(provider.serve))
	t.Cleanup(provider.server.Close)
	provider.caFile = filepath.Join(t.TempDir(), "provider-ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: provider.server.Certificate().Raw})
	if err := os.WriteFile(provider.caFile, certificate, 0o600); err != nil {
		t.Fatal(err)
	}
	return provider
}

func (p *syntheticCognitoProvider) serve(w http.ResponseWriter, r *http.Request) {
	origin := p.server.URL
	switch r.URL.Path {
	case "/pool/.well-known/openid-configuration":
		if status := p.discoveryStatus.Load(); status != 0 {
			w.WriteHeader(int(status))
			return
		}
		issuer := origin + "/pool"
		if p.issuerMismatch.Load() {
			issuer = origin + "/other-pool"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": issuer, "authorization_endpoint": origin + "/oauth2/authorize",
			"token_endpoint": origin + "/oauth2/token", "jwks_uri": origin + "/pool/.well-known/jwks.json",
			"end_session_endpoint": origin + "/logout", "id_token_signing_alg_values_supported": []string{"RS256"},
		})
	case "/pool/.well-known/jwks.json":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "synthetic-key",
			"n": base64.RawURLEncoding.EncodeToString(p.key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(p.key.E)).Bytes()),
		}}})
	case "/oauth2/token":
		p.token(w, r)
	default:
		http.NotFound(w, r)
	}
}

// authorize plays the managed-login page: it validates the request and returns state and a one-use code.
func (p *syntheticCognitoProvider) authorize(t *testing.T, authorizationURL string, groups ...string) (string, string) {
	t.Helper()
	parsed, err := url.Parse(authorizationURL)
	if err != nil || parsed.Scheme+"://"+parsed.Host != p.server.URL || parsed.Path != "/oauth2/authorize" {
		t.Fatal("authorization URL is not the configured endpoint")
	}
	query := parsed.Query()
	if query.Get("response_type") != "code" || query.Get("client_id") != syntheticCognitoClientID ||
		query.Get("scope") != "openid" || query.Get("code_challenge_method") != "S256" ||
		query.Get("state") == "" || query.Get("nonce") == "" || query.Get("code_challenge") == "" {
		t.Fatal("authorization request is missing PKCE, state, nonce or openid")
	}
	code := rand.Text()
	p.mu.Lock()
	p.codes[code] = syntheticGrant{nonce: query.Get("nonce"), challenge: query.Get("code_challenge"),
		redirect: query.Get("redirect_uri"), groups: groups}
	p.mu.Unlock()
	return query.Get("state"), code
}

func (p *syntheticCognitoProvider) token(w http.ResponseWriter, r *http.Request) {
	p.tokenCalls.Add(1)
	if status := p.tokenStatus.Load(); status != 0 {
		w.WriteHeader(int(status))
		return
	}
	client, secret, ok := r.BasicAuth()
	if r.Method != http.MethodPost || !ok || client != syntheticCognitoClientID || secret != syntheticCognitoSecret || r.ParseForm() != nil {
		http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
		return
	}
	p.mu.Lock()
	var grant syntheticGrant
	var found bool
	refresh := ""
	switch r.PostForm.Get("grant_type") {
	case "authorization_code":
		grant, found = p.codes[r.PostForm.Get("code")]
		delete(p.codes, r.PostForm.Get("code"))
		digest := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		found = found && base64.RawURLEncoding.EncodeToString(digest[:]) == grant.challenge &&
			r.PostForm.Get("redirect_uri") == grant.redirect
		if found {
			refresh = rand.Text()
			p.refreshTokens[refresh] = grant
		}
	case "refresh_token":
		grant, found = p.refreshTokens[r.PostForm.Get("refresh_token")]
		grant.nonce = ""
	}
	p.mu.Unlock()
	if !found {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		return
	}
	if p.wrongNonce.Load() && grant.nonce != "" {
		grant.nonce = "a-different-nonce"
	}
	issuedAt := time.Now().Add(-2 * time.Second)
	if refresh == "" {
		issuedAt = time.Now()
	}
	claims := map[string]any{
		"iss": p.server.URL + "/pool", "sub": "synthetic|subject", "aud": syntheticCognitoClientID, "token_use": "id",
		"iat": issuedAt.Unix(), "auth_time": issuedAt.Unix(), "exp": issuedAt.Add(time.Hour).Unix(),
	}
	if len(grant.groups) > 0 {
		claims["cognito:groups"] = grant.groups
	}
	if grant.nonce != "" {
		claims["nonce"] = grant.nonce
	}
	response := map[string]any{"access_token": "synthetic-access", "expires_in": 3600, "token_type": "Bearer",
		"id_token": p.sign(claims)}
	if refresh != "" {
		response["refresh_token"] = refresh
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (p *syntheticCognitoProvider) sign(claims map[string]any) string {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": "synthetic-key", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	message := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(message))
	signature, err := rsa.SignPKCS1v15(rand.Reader, p.key, crypto.SHA256, digest[:])
	if err != nil {
		panic(err)
	}
	return message + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func (p *syntheticCognitoProvider) environment() map[string]string {
	origin := p.server.URL
	return map[string]string{
		cognitoIssuerEnv: origin + "/pool", cognitoClientIDEnv: syntheticCognitoClientID,
		cognitoAuthorizationEnv: origin + "/oauth2/authorize", cognitoTokenEnv: origin + "/oauth2/token",
		cognitoJWKSEnv: origin + "/pool/.well-known/jwks.json", cognitoLogoutEnv: origin + "/logout",
		cognitoAuthenticationEnv: "basic", cognitoClientSecretEnv: syntheticCognitoSecret,
		cognitoAttemptKeyEnv: syntheticAttemptKey, cognitoRefreshKeyEnv: syntheticRefreshKey, cognitoCAFileEnv: p.caFile,
	}
}
