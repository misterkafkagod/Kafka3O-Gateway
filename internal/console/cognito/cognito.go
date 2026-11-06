package cognito

import (
	"crypto/x509"
	"errors"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

const (
	maxProviderResponseBytes = 4 << 20
	maxIDTokenBytes          = 1 << 20
)

type ProviderConfig struct {
	Issuer           string
	ClientID         string
	AuthorizationURL string
	TokenURL         string
	JWKSURL          string
}

type VerifiedIdentity struct {
	Issuer    string
	Subject   string
	Groups    []string
	ExpiresAt time.Time
}

type Verifier struct {
	verifier *oidc.IDTokenVerifier
	config   ProviderConfig
	issuer   string
	clientID string
	tokenURL string
	roots    *x509.CertPool
	now      func() time.Time
}

type verificationAttempt struct {
	keyError error
}

type verificationAttemptKey struct{}

var (
	ErrInvalidProviderConfig = errors.New("invalid Cognito provider configuration")
	ErrInvalidProvider       = errors.New("Cognito discovery does not match configured provider")
	ErrProviderUnavailable   = errors.New("Cognito provider verification is unavailable")
	ErrInvalidIDToken        = errors.New("Cognito ID token is invalid")
)
