package config

import (
	"crypto/tls"
	"errors"
	"regexp"
)

var bearerTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+={0,2}$`)

func LoadGatewaySecurity(tlsConfig TLSConfig, token string, loadCertificate func(string, string) (tls.Certificate, error)) (tls.Certificate, error) {
	if len(token) < 32 || !bearerTokenPattern.MatchString(token) {
		return tls.Certificate{}, errors.New("KAFKA3O_GATEWAY_TOKEN must contain at least 32 bearer-token characters")
	}
	return LoadGatewayCertificate(tlsConfig, loadCertificate)
}
