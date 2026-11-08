package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
)

type policyConfig struct {
	listen string
	tls    *tls.Config
}

// loadPolicyConfig reads the optional mutual-TLS policy listener for secured Gateways. Setting any of its
// variables requires all of them.
func loadPolicyConfig(getenv func(string) string) (policyConfig, bool, error) {
	listen := getenv("KAFKA3O_CONSOLE_POLICY_LISTEN")
	certificate := getenv("KAFKA3O_CONSOLE_POLICY_TLS_CERT_FILE")
	key := getenv("KAFKA3O_CONSOLE_POLICY_TLS_KEY_FILE")
	ca := getenv("KAFKA3O_CONSOLE_SERVICE_CA_FILE")
	if listen == "" && certificate == "" && key == "" && ca == "" {
		return policyConfig{}, false, nil
	}
	if _, port, err := net.SplitHostPort(listen); err != nil || port == "" || strings.Contains(listen, "/") {
		return policyConfig{}, false, fmt.Errorf("%w: the policy listener needs a host:port address", errInvalidServerConfig)
	}
	for _, path := range []string{certificate, key, ca} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return policyConfig{}, false, fmt.Errorf("%w: policy TLS and service CA paths must be absolute", errInvalidServerConfig)
		}
	}
	identity, pool, err := servicetls.LoadServiceIdentity(certificate, key, ca, servicetls.ConsoleURI)
	if err != nil {
		return policyConfig{}, false, err
	}
	return policyConfig{listen: listen, tls: &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
		VerifyPeerCertificate: servicetls.RequirePeerURI(func(uri string) bool { return strings.HasPrefix(uri, servicetls.GatewayURIPrefix) }),
	}}, true, nil
}
