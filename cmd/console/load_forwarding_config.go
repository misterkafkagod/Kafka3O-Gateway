package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"path/filepath"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

type forwardingConfig struct {
	identity  tls.Certificate
	serviceCA *x509.CertPool
}

// loadForwardingConfig reads the optional Console client identity used to call secured Gateways; it needs the
// shared service CA as well. Setting either certificate variable requires both and the CA.
func loadForwardingConfig(getenv func(string) string) (forwardingConfig, bool, error) {
	certificate := getenv("KAFKA3O_CONSOLE_GATEWAY_CLIENT_CERT_FILE")
	key := getenv("KAFKA3O_CONSOLE_GATEWAY_CLIENT_KEY_FILE")
	ca := getenv("KAFKA3O_CONSOLE_SERVICE_CA_FILE")
	if certificate == "" && key == "" {
		return forwardingConfig{}, false, nil
	}
	for _, path := range []string{certificate, key, ca} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return forwardingConfig{}, false, fmt.Errorf("%w: Gateway client certificate, key and service CA paths must be absolute", errInvalidServerConfig)
		}
	}
	identity, pool, err := servicetls.LoadServiceIdentity(certificate, key, ca, servicetls.ConsoleURI)
	if err != nil {
		return forwardingConfig{}, false, err
	}
	return forwardingConfig{identity: identity, serviceCA: pool}, true, nil
}
