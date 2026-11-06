package cognito

import (
	"crypto/sha256"
	"strings"
)

func authorizationProviderBinding(config ProviderConfig, callbackURL string) ([32]byte, error) {
	if !validProviderConfig(config) || !validHTTPSURL(callbackURL) {
		return [32]byte{}, ErrInvalidAttemptConfig
	}
	return sha256.Sum256([]byte(strings.Join([]string{
		config.Issuer, config.ClientID, config.AuthorizationURL, config.TokenURL, config.JWKSURL, callbackURL,
	}, "\x00"))), nil
}
