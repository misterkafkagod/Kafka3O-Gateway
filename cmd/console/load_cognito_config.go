package main

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

var cognitoClientIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func loadCognitoConfig(getenv func(string) string, origin string) (cognitoConfig, bool, error) {
	values := make(map[string]string, len(cognitoSettingNames))
	configured := false
	for _, name := range cognitoSettingNames {
		values[name] = getenv(name)
		configured = configured || values[name] != ""
	}
	if !configured {
		return cognitoConfig{}, false, nil
	}
	for _, name := range []string{cognitoIssuerEnv, cognitoClientIDEnv, cognitoAuthorizationEnv, cognitoTokenEnv,
		cognitoJWKSEnv, cognitoLogoutEnv, cognitoAuthenticationEnv, cognitoAttemptKeyEnv, cognitoRefreshKeyEnv} {
		if values[name] == "" {
			return cognitoConfig{}, false, fmt.Errorf("%w: %s is required when Cognito is configured", errInvalidCognitoConfig, name)
		}
	}
	issuer, ok := validCognitoURL(values[cognitoIssuerEnv])
	if !ok {
		return cognitoConfig{}, false, fmt.Errorf("%w: the issuer must be an HTTPS URL without query or fragment", errInvalidCognitoConfig)
	}
	login := make(map[string]string, 3)
	for _, name := range []string{cognitoAuthorizationEnv, cognitoTokenEnv, cognitoLogoutEnv} {
		parsed, ok := validCognitoURL(values[name])
		if !ok {
			return cognitoConfig{}, false, fmt.Errorf("%w: %s must be an HTTPS URL without query or fragment", errInvalidCognitoConfig, name)
		}
		login[name] = parsed.Host
	}
	if login[cognitoTokenEnv] != login[cognitoAuthorizationEnv] || login[cognitoLogoutEnv] != login[cognitoAuthorizationEnv] {
		return cognitoConfig{}, false, fmt.Errorf("%w: authorization, token and logout URLs must share one login domain", errInvalidCognitoConfig)
	}
	if logout, _ := validCognitoURL(values[cognitoLogoutEnv]); logout.Path != "/logout" {
		return cognitoConfig{}, false, fmt.Errorf("%w: the logout URL must be the provider's /logout endpoint", errInvalidCognitoConfig)
	}
	jwks, ok := validCognitoURL(values[cognitoJWKSEnv])
	if !ok || jwks.Host != issuer.Host || !strings.HasPrefix(jwks.Path, strings.TrimSuffix(issuer.Path, "/")+"/") {
		return cognitoConfig{}, false, fmt.Errorf("%w: the JWKS URL must be an HTTPS URL under the issuer", errInvalidCognitoConfig)
	}
	if !cognitoClientIDPattern.MatchString(values[cognitoClientIDEnv]) {
		return cognitoConfig{}, false, fmt.Errorf("%w: the client ID is malformed", errInvalidCognitoConfig)
	}
	authentication := cognito.ClientAuthentication(values[cognitoAuthenticationEnv])
	secret := values[cognitoClientSecretEnv]
	switch {
	case authentication == cognito.ClientAuthenticationBasic && secret != "" && len(secret) <= 4096:
	case authentication == cognito.ClientAuthenticationPublic && secret == "":
	default:
		return cognitoConfig{}, false, fmt.Errorf("%w: client authentication must be basic with a secret or public without one", errInvalidCognitoConfig)
	}
	attemptKey, ok := decodeCognitoKey(values[cognitoAttemptKeyEnv])
	if !ok {
		return cognitoConfig{}, false, fmt.Errorf("%w: the attempt key must be 32 nonzero bytes in unpadded base64url", errInvalidCognitoConfig)
	}
	refreshKey, ok := decodeCognitoKey(values[cognitoRefreshKeyEnv])
	if !ok {
		return cognitoConfig{}, false, fmt.Errorf("%w: the refresh-token key must be 32 nonzero bytes in unpadded base64url", errInvalidCognitoConfig)
	}
	if bytes.Equal(attemptKey, refreshKey) {
		return cognitoConfig{}, false, fmt.Errorf("%w: the attempt and refresh-token keys must differ", errInvalidCognitoConfig)
	}
	roots, err := loadCognitoRoots(values[cognitoCAFileEnv])
	if err != nil {
		return cognitoConfig{}, false, err
	}
	return cognitoConfig{
		provider: cognito.ProviderConfig{
			Issuer: values[cognitoIssuerEnv], ClientID: values[cognitoClientIDEnv],
			AuthorizationURL: values[cognitoAuthorizationEnv], TokenURL: values[cognitoTokenEnv], JWKSURL: values[cognitoJWKSEnv],
		},
		logoutURL: values[cognitoLogoutEnv], callbackURL: origin + "/auth/cognito/callback", logoutReturn: origin + "/",
		authentication: authentication, clientSecret: secret, attemptKey: attemptKey, refreshKey: refreshKey, roots: roots,
	}, true, nil
}
