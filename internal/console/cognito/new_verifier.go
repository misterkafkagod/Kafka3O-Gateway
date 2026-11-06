package cognito

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

func NewVerifier(ctx context.Context, config ProviderConfig, roots *x509.CertPool, now func() time.Time) (*Verifier, error) {
	if ctx == nil || now == nil || !validProviderConfig(config) {
		return nil, ErrInvalidProviderConfig
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, ErrInvalidProviderConfig
	}
	if roots != nil {
		roots = roots.Clone()
	}
	transport := base.Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	discoveryURL := strings.TrimSuffix(config.Issuer, "/") + "/.well-known/openid-configuration"
	client := &http.Client{
		Transport: providerTransport{base: transport, discoveryURL: discoveryURL, jwksURL: config.JWKSURL},
		Timeout:   10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), config.Issuer)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var mismatch *oidc.IssuerMismatchError
		if errors.As(err, &mismatch) {
			return nil, ErrInvalidProvider
		}
		return nil, ErrProviderUnavailable
	}
	var metadata struct {
		JWKSURL    string   `json:"jwks_uri"`
		Algorithms []string `json:"id_token_signing_alg_values_supported"`
	}
	endpoints := provider.Endpoint()
	if err := provider.Claims(&metadata); err != nil || endpoints.AuthURL != config.AuthorizationURL ||
		endpoints.TokenURL != config.TokenURL || metadata.JWKSURL != config.JWKSURL ||
		!slices.Contains(metadata.Algorithms, oidc.RS256) {
		return nil, ErrInvalidProvider
	}
	keyContext := oidc.ClientContext(context.Background(), client)
	keys := oidc.NewRemoteKeySet(keyContext, config.JWKSURL)
	verifier := oidc.NewVerifier(config.Issuer, trackingKeySet{keys: keys}, &oidc.Config{
		ClientID: config.ClientID, SupportedSigningAlgs: []string{oidc.RS256}, Now: now,
	})
	return &Verifier{
		verifier: verifier, config: config, issuer: config.Issuer, clientID: config.ClientID,
		tokenURL: config.TokenURL, roots: roots, now: now,
	}, nil
}
