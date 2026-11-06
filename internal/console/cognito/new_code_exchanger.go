package cognito

import (
	"crypto/tls"
	"net/http"
	"time"

	"golang.org/x/oauth2"
)

func NewCodeExchanger(attempts *AttemptManager, verifier *Verifier, callbackURL string,
	authentication ClientAuthentication, clientSecret string) (*CodeExchanger, error) {
	if attempts == nil || verifier == nil || verifier.verifier == nil || verifier.clientID == "" ||
		!validHTTPSURL(verifier.tokenURL) || !validHTTPSURL(callbackURL) || len(clientSecret) > 4096 {
		return nil, ErrCodeExchangeConfig
	}
	store, ok := attempts.store.(CodeExchangeStore)
	if !ok {
		return nil, ErrCodeExchangeConfig
	}
	binding, err := authorizationProviderBinding(verifier.config, callbackURL)
	if err != nil || binding != attempts.providerBinding {
		return nil, ErrCodeExchangeConfig
	}
	var style oauth2.AuthStyle
	switch authentication {
	case ClientAuthenticationPublic:
		if clientSecret != "" {
			return nil, ErrCodeExchangeConfig
		}
		style = oauth2.AuthStyleInParams
	case ClientAuthenticationBasic:
		if clientSecret == "" {
			return nil, ErrCodeExchangeConfig
		}
		style = oauth2.AuthStyleInHeader
	default:
		return nil, ErrCodeExchangeConfig
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, ErrCodeExchangeConfig
	}
	transport := base.Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: verifier.roots}
	client := &http.Client{
		Transport: tokenTransport{base: transport, tokenURL: verifier.tokenURL},
		Timeout:   10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &CodeExchanger{
		attempts: attempts, store: store, verifier: verifier, client: client,
		oauth: oauth2.Config{
			ClientID: verifier.clientID, ClientSecret: clientSecret, RedirectURL: callbackURL,
			Endpoint: oauth2.Endpoint{TokenURL: verifier.tokenURL, AuthStyle: style},
		},
	}, nil
}
