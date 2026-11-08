package main

import (
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestGatewayForwardingConfiguration(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	gateway := ca.Issue(t, "gateway", servicetls.GatewayURIPrefix+"dev", time.Now().Add(time.Hour))
	env := func(values map[string]string) func(string) string { return func(key string) string { return values[key] } }
	if _, configured, err := loadForwardingConfig(env(nil)); err != nil || configured {
		t.Fatalf("unset = %t %v", configured, err)
	}
	config, configured, err := loadForwardingConfig(env(map[string]string{
		"KAFKA3O_CONSOLE_GATEWAY_CLIENT_CERT_FILE": console.CertificateFile, "KAFKA3O_CONSOLE_GATEWAY_CLIENT_KEY_FILE": console.KeyFile,
		"KAFKA3O_CONSOLE_SERVICE_CA_FILE": ca.CAFile,
	}))
	if err != nil || !configured || config.serviceCA == nil || config.identity.Leaf == nil {
		t.Fatalf("configured = %t %v", configured, err)
	}
	for name, values := range map[string]map[string]string{
		"missing CA":       {"KAFKA3O_CONSOLE_GATEWAY_CLIENT_CERT_FILE": console.CertificateFile, "KAFKA3O_CONSOLE_GATEWAY_CLIENT_KEY_FILE": console.KeyFile},
		"missing key":      {"KAFKA3O_CONSOLE_GATEWAY_CLIENT_CERT_FILE": console.CertificateFile, "KAFKA3O_CONSOLE_SERVICE_CA_FILE": ca.CAFile},
		"gateway identity": {"KAFKA3O_CONSOLE_GATEWAY_CLIENT_CERT_FILE": gateway.CertificateFile, "KAFKA3O_CONSOLE_GATEWAY_CLIENT_KEY_FILE": gateway.KeyFile, "KAFKA3O_CONSOLE_SERVICE_CA_FILE": ca.CAFile},
	} {
		if _, _, err := loadForwardingConfig(env(values)); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}
