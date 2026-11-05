package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
)

var bearerTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+={0,2}$`)

func LoadGatewaySecurity(tlsConfig TLSConfig, token string, loadCertificate func(string, string) (tls.Certificate, error)) (tls.Certificate, error) {
	if len(token) < 32 || !bearerTokenPattern.MatchString(token) {
		return tls.Certificate{}, errors.New("KAFKA3O_GATEWAY_TOKEN must contain at least 32 bearer-token characters")
	}
	if tlsConfig.CertificateFile == "" || tlsConfig.KeyFile == "" {
		return tls.Certificate{}, errors.New("http.tls.certificateFile and http.tls.keyFile are required")
	}
	certificate, err := loadCertificate(tlsConfig.CertificateFile, tlsConfig.KeyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load gateway TLS certificate and key: %w", err)
	}
	return certificate, nil
}
