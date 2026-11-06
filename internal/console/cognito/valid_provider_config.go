package cognito

import (
	"strings"
	"unicode"
)

func validProviderConfig(config ProviderConfig) bool {
	return validHTTPSURL(config.Issuer) && validHTTPSURL(config.AuthorizationURL) &&
		validHTTPSURL(config.TokenURL) && validHTTPSURL(config.JWKSURL) && config.ClientID != "" &&
		strings.TrimSpace(config.ClientID) == config.ClientID && len(config.ClientID) <= 256 &&
		strings.IndexFunc(config.ClientID, unicode.IsControl) < 0
}
