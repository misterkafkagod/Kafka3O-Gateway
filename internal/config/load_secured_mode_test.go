package config

import (
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/servicetls/servicetlstest"
)

func TestLoadSecuredModeValidatesIdentityAndPolicyURL(t *testing.T) {
	ca := servicetlstest.NewAuthority(t)
	gateway := ca.Issue(t, "gateway-prod", servicetls.GatewayURIPrefix+"prod", time.Now().Add(time.Hour))
	valid := SecurityConfig{Mode: "secured", ClusterID: "prod", ServiceCAFile: ca.CAFile,
		ClientCertificateFile: gateway.CertificateFile, ClientKeyFile: gateway.KeyFile, PolicyURL: "https://console.example:9443"}
	if _, secured, err := LoadSecuredMode(SecurityConfig{}); err != nil || secured {
		t.Fatalf("default mode = %t %v", secured, err)
	}
	mode, secured, err := LoadSecuredMode(valid)
	if err != nil || !secured || mode.ClusterID != "prod" || mode.PolicyURL != "https://console.example:9443" || mode.ServiceCA == nil {
		t.Fatalf("secured mode = %+v %t %v", mode, secured, err)
	}
	for name, change := range map[string]func(*SecurityConfig){
		"unknown mode":       func(c *SecurityConfig) { c.Mode = "token" },
		"bad cluster":        func(c *SecurityConfig) { c.ClusterID = "prod/1" },
		"other cluster cert": func(c *SecurityConfig) { c.ClusterID = "dev" },
		"http policy":        func(c *SecurityConfig) { c.PolicyURL = "http://console.example" },
		"policy path":        func(c *SecurityConfig) { c.PolicyURL = "https://console.example/other" },
		"relative CA":        func(c *SecurityConfig) { c.ServiceCAFile = "ca.pem" },
		"missing client key": func(c *SecurityConfig) { c.ClientKeyFile = "" },
	} {
		config := valid
		change(&config)
		if _, _, err := LoadSecuredMode(config); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}
