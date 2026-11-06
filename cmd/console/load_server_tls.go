package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"time"
)

func loadServerTLS(config serverConfig) (*tls.Config, error) {
	keyInfo, err := os.Stat(config.key)
	if err != nil {
		return nil, fmt.Errorf("inspect Console TLS key: %w", err)
	}
	if !keyInfo.Mode().IsRegular() || (runtime.GOOS != "windows" && keyInfo.Mode().Perm()&0077 != 0) {
		return nil, fmt.Errorf("%w: TLS key must be an owner-only regular file", errInvalidServerConfig)
	}
	certificate, err := tls.LoadX509KeyPair(config.certificate, config.key)
	if err != nil {
		return nil, fmt.Errorf("load Console TLS certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse Console TLS certificate: %w", err)
	}
	origin, err := url.Parse(config.origin)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HTTPS origin", errInvalidServerConfig)
	}
	if err := leaf.VerifyHostname(origin.Hostname()); err != nil {
		return nil, fmt.Errorf("%w: TLS certificate does not match the Console origin", errInvalidServerConfig)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, fmt.Errorf("%w: TLS certificate is not currently valid", errInvalidServerConfig)
	}
	if len(leaf.ExtKeyUsage) != 0 || len(leaf.UnknownExtKeyUsage) != 0 {
		allowed := false
		for _, usage := range leaf.ExtKeyUsage {
			if usage == x509.ExtKeyUsageAny || usage == x509.ExtKeyUsageServerAuth {
				allowed = true
			}
		}
		if !allowed {
			return nil, fmt.Errorf("%w: TLS certificate does not permit server authentication", errInvalidServerConfig)
		}
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}, nil
}
