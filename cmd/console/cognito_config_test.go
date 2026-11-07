package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

const cognitoTestOrigin = "https://console.example"

func TestCognitoConfigIsOptional(t *testing.T) {
	config, configured, err := loadCognitoConfig(func(string) string { return "" }, cognitoTestOrigin)
	if err != nil || configured || config.provider != (cognito.ProviderConfig{}) {
		t.Fatalf("absent Cognito settings were not treated as disabled: configured=%t err=%v", configured, err)
	}
}

func TestCognitoConfigDerivesOriginBoundURLs(t *testing.T) {
	provider := newSyntheticCognitoProvider(t)
	values := provider.environment()
	config, configured, err := loadCognitoConfig(func(name string) string { return values[name] }, cognitoTestOrigin)
	if err != nil || !configured {
		t.Fatalf("valid Cognito settings were rejected: %v", err)
	}
	if config.callbackURL != cognitoTestOrigin+"/auth/cognito/callback" || config.logoutReturn != cognitoTestOrigin+"/" ||
		config.authentication != cognito.ClientAuthenticationBasic || config.clientSecret != syntheticCognitoSecret ||
		config.roots == nil || config.provider.ClientID != syntheticCognitoClientID || len(config.attemptKey) != 32 ||
		len(config.refreshKey) != 32 {
		t.Fatal("valid Cognito settings produced an unexpected configuration")
	}
	values[cognitoAuthenticationEnv], values[cognitoClientSecretEnv], values[cognitoCAFileEnv] = "public", "", ""
	config, configured, err = loadCognitoConfig(func(name string) string { return values[name] }, cognitoTestOrigin)
	if err != nil || !configured || config.authentication != cognito.ClientAuthenticationPublic || config.roots != nil {
		t.Fatalf("public client with system roots was rejected: %v", err)
	}
}

func TestCognitoConfigRejectsPartialAndUnsafeSettings(t *testing.T) {
	provider := newSyntheticCognitoProvider(t)
	origin := provider.server.URL
	notPEM := filepath.Join(t.TempDir(), "not.pem")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		change map[string]string
	}{
		{"only issuer", nil},
		{"insecure issuer", map[string]string{cognitoIssuerEnv: "http" + strings.TrimPrefix(origin, "https") + "/pool"}},
		{"token query", map[string]string{cognitoTokenEnv: origin + "/oauth2/token?x=1"}},
		{"authorization user info", map[string]string{cognitoAuthorizationEnv: strings.Replace(origin, "https://", "https://user@", 1) + "/oauth2/authorize"}},
		{"logout other host", map[string]string{cognitoLogoutEnv: "https://logout.example/logout"}},
		{"logout wrong path", map[string]string{cognitoLogoutEnv: origin + "/signout"}},
		{"JWKS other host", map[string]string{cognitoJWKSEnv: "https://keys.example/pool/.well-known/jwks.json"}},
		{"JWKS outside issuer", map[string]string{cognitoJWKSEnv: origin + "/other/.well-known/jwks.json"}},
		{"malformed client ID", map[string]string{cognitoClientIDEnv: "client id"}},
		{"unknown authentication", map[string]string{cognitoAuthenticationEnv: "post"}},
		{"basic without secret", map[string]string{cognitoClientSecretEnv: ""}},
		{"public with secret", map[string]string{cognitoAuthenticationEnv: "public"}},
		{"short attempt key", map[string]string{cognitoAttemptKeyEnv: syntheticAttemptKey[:20]}},
		{"padded refresh key", map[string]string{cognitoRefreshKeyEnv: syntheticRefreshKey + "="}},
		{"zero attempt key", map[string]string{cognitoAttemptKeyEnv: strings.Repeat("A", 43)}},
		{"reused key", map[string]string{cognitoRefreshKeyEnv: syntheticAttemptKey}},
		{"relative CA file", map[string]string{cognitoCAFileEnv: "provider-ca.pem"}},
		{"missing CA file", map[string]string{cognitoCAFileEnv: filepath.Join(t.TempDir(), "missing.pem")}},
		{"CA file without PEM", map[string]string{cognitoCAFileEnv: notPEM}},
	}
	for _, name := range []string{cognitoIssuerEnv, cognitoClientIDEnv, cognitoAuthorizationEnv, cognitoTokenEnv, cognitoJWKSEnv,
		cognitoLogoutEnv, cognitoAuthenticationEnv, cognitoAttemptKeyEnv, cognitoRefreshKeyEnv} {
		cases = append(cases, struct {
			name   string
			change map[string]string
		}{"missing " + name, map[string]string{name: ""}})
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			values := provider.environment()
			if tt.change == nil {
				values = map[string]string{cognitoIssuerEnv: values[cognitoIssuerEnv]}
			}
			for name, value := range tt.change {
				values[name] = value
			}
			_, configured, err := loadCognitoConfig(func(name string) string { return values[name] }, cognitoTestOrigin)
			if !errors.Is(err, errInvalidCognitoConfig) || configured {
				t.Fatalf("unsafe Cognito settings were accepted: configured=%t err=%v", configured, err)
			}
			for _, secret := range []string{syntheticCognitoSecret, syntheticAttemptKey, syntheticRefreshKey} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("Cognito configuration error exposed a secret")
				}
			}
		})
	}
}
