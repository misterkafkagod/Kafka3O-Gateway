package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

const (
	syntheticCognitoClientID = "synthetic-client"
	syntheticCognitoSecret   = "synthetic-client-secret-value"
)

var (
	syntheticAttemptKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x31}, 32))
	syntheticRefreshKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x52}, 32))
)

// syntheticCognitoProvider is a verified-HTTPS Cognito-compatible discovery and JWKS endpoint.
type syntheticCognitoProvider struct {
	server          *httptest.Server
	caFile          string
	key             *rsa.PrivateKey
	discoveryStatus atomic.Int32
	issuerMismatch  atomic.Bool
}

func newSyntheticCognitoProvider(t *testing.T) *syntheticCognitoProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	provider := &syntheticCognitoProvider{key: key}
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
	default:
		http.NotFound(w, r)
	}
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
