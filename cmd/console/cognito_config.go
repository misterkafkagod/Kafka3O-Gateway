package main

import (
	"crypto/x509"
	"errors"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

type cognitoConfig struct {
	provider       cognito.ProviderConfig
	logoutURL      string
	callbackURL    string
	logoutReturn   string
	authentication cognito.ClientAuthentication
	clientSecret   string
	attemptKey     []byte
	refreshKey     []byte
	roots          *x509.CertPool
}

const (
	cognitoIssuerEnv         = "KAFKA3O_CONSOLE_COGNITO_ISSUER"
	cognitoClientIDEnv       = "KAFKA3O_CONSOLE_COGNITO_CLIENT_ID"
	cognitoAuthorizationEnv  = "KAFKA3O_CONSOLE_COGNITO_AUTHORIZATION_URL"
	cognitoTokenEnv          = "KAFKA3O_CONSOLE_COGNITO_TOKEN_URL"
	cognitoJWKSEnv           = "KAFKA3O_CONSOLE_COGNITO_JWKS_URL"
	cognitoLogoutEnv         = "KAFKA3O_CONSOLE_COGNITO_LOGOUT_URL"
	cognitoAuthenticationEnv = "KAFKA3O_CONSOLE_COGNITO_CLIENT_AUTHENTICATION"
	cognitoClientSecretEnv   = "KAFKA3O_CONSOLE_COGNITO_CLIENT_SECRET"
	cognitoAttemptKeyEnv     = "KAFKA3O_CONSOLE_COGNITO_ATTEMPT_KEY"
	cognitoRefreshKeyEnv     = "KAFKA3O_CONSOLE_COGNITO_REFRESH_TOKEN_KEY"
	cognitoCAFileEnv         = "KAFKA3O_CONSOLE_COGNITO_CA_FILE"
)

var cognitoSettingNames = []string{
	cognitoIssuerEnv, cognitoClientIDEnv, cognitoAuthorizationEnv, cognitoTokenEnv, cognitoJWKSEnv, cognitoLogoutEnv,
	cognitoAuthenticationEnv, cognitoClientSecretEnv, cognitoAttemptKeyEnv, cognitoRefreshKeyEnv, cognitoCAFileEnv,
}

var errInvalidCognitoConfig = errors.New("invalid Console Cognito configuration")
