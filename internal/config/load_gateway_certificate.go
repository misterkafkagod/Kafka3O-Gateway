package config

import (
	"crypto/tls"
	"errors"
	"fmt"
)

// LoadGatewayCertificate loads the Gateway's HTTPS server certificate.
func LoadGatewayCertificate(tlsConfig TLSConfig, loadCertificate func(string, string) (tls.Certificate, error)) (tls.Certificate, error) {
	if tlsConfig.CertificateFile == "" || tlsConfig.KeyFile == "" {
		return tls.Certificate{}, errors.New("http.tls.certificateFile and http.tls.keyFile are required")
	}
	certificate, err := loadCertificate(tlsConfig.CertificateFile, tlsConfig.KeyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load gateway TLS certificate and key: %w", err)
	}
	return certificate, nil
}
