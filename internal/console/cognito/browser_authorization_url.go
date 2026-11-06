package cognito

import (
	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

func (e *CodeExchanger) BrowserAuthorizationURL(start AuthorizationStart) (string, error) {
	if e == nil || e.verifier == nil {
		return "", ErrInvalidAttempt
	}
	if _, err := e.attempts.validateBrowserAuthorization(start); err != nil {
		return "", err
	}
	config := e.oauth
	config.Endpoint.AuthURL = e.verifier.config.AuthorizationURL
	config.Scopes = []string{oidc.ScopeOpenID}
	return config.AuthCodeURL(start.State,
		oauth2.SetAuthURLParam("nonce", start.Nonce),
		oauth2.SetAuthURLParam("code_challenge", start.CodeChallenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	), nil
}
