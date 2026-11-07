package main

import (
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

func loadCognitoRoots(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("%w: the CA file path must be absolute", errInvalidCognitoConfig)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: the CA file could not be read", errInvalidCognitoConfig)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("%w: the CA file has no PEM certificates", errInvalidCognitoConfig)
	}
	return roots, nil
}
