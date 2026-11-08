package main

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestPolicyListenerConfiguration(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	console := ca.Issue(t, "console", servicetls.ConsoleURI, time.Now().Add(time.Hour))
	gateway := ca.Issue(t, "gateway", servicetls.GatewayURIPrefix+"prod", time.Now().Add(time.Hour))
	env := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}
	full := map[string]string{
		"KAFKA3O_CONSOLE_POLICY_LISTEN": "0.0.0.0:9443", "KAFKA3O_CONSOLE_POLICY_TLS_CERT_FILE": console.CertificateFile,
		"KAFKA3O_CONSOLE_POLICY_TLS_KEY_FILE": console.KeyFile, "KAFKA3O_CONSOLE_SERVICE_CA_FILE": ca.CAFile,
	}
	if _, configured, err := loadPolicyConfig(env(nil)); err != nil || configured {
		t.Fatalf("unset policy config = %t %v", configured, err)
	}
	config, configured, err := loadPolicyConfig(env(full))
	if err != nil || !configured || config.listen != "0.0.0.0:9443" || config.tls.MinVersion != tls.VersionTLS13 ||
		config.tls.ClientAuth != tls.RequireAndVerifyClientCert || config.tls.ClientCAs == nil {
		t.Fatalf("policy config = %+v %t %v", config, configured, err)
	}
	if config.tls.VerifyPeerCertificate(nil, nil) == nil {
		t.Fatal("unverified peers passed")
	}
	for name, change := range map[string][2]string{
		"partial":            {"KAFKA3O_CONSOLE_POLICY_TLS_KEY_FILE", ""},
		"relative CA":        {"KAFKA3O_CONSOLE_SERVICE_CA_FILE", "ca.pem"},
		"bad listen":         {"KAFKA3O_CONSOLE_POLICY_LISTEN", "9443"},
		"gateway as console": {"KAFKA3O_CONSOLE_POLICY_TLS_CERT_FILE", gateway.CertificateFile},
	} {
		values := map[string]string{}
		for key, value := range full {
			values[key] = value
		}
		values[change[0]] = change[1]
		if name == "gateway as console" {
			values["KAFKA3O_CONSOLE_POLICY_TLS_KEY_FILE"] = gateway.KeyFile
		}
		if _, _, err := loadPolicyConfig(env(values)); err == nil || (!errors.Is(err, errInvalidServerConfig) && !errors.Is(err, servicetls.ErrInvalidServiceTLS)) {
			t.Fatalf("%s error = %v", name, err)
		}
	}
}
