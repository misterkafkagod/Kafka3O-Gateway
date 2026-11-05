package config

import (
	"strings"
	"testing"
)

func TestValidateKafkaSecurity(t *testing.T) {
	tests := []struct {
		name  string
		cfg   KafkaConfig
		valid bool
	}{
		{name: "plaintext", valid: true},
		{name: "TLS with system roots", cfg: KafkaConfig{TLS: KafkaTLSConfig{Enabled: true}}, valid: true},
		{name: "TLS with CA", cfg: KafkaConfig{TLS: KafkaTLSConfig{Enabled: true, CAFile: "ca.pem"}}, valid: true},
		{name: "CA without TLS", cfg: KafkaConfig{TLS: KafkaTLSConfig{CAFile: "ca.pem"}}},
		{name: "username without SASL", cfg: KafkaConfig{SASL: KafkaSASLConfig{Username: "test-user"}}},
		{name: "password without SASL", cfg: KafkaConfig{SASL: KafkaSASLConfig{Password: "synthetic-test-password"}}},
		{name: "missing username", cfg: KafkaConfig{SASL: KafkaSASLConfig{Mechanism: "PLAIN", Password: "synthetic-test-password"}}},
		{name: "missing password", cfg: KafkaConfig{SASL: KafkaSASLConfig{Mechanism: "PLAIN", Username: "test-user"}}},
		{name: "blank mechanism", cfg: KafkaConfig{SASL: KafkaSASLConfig{Mechanism: " "}}},
		{name: "unknown mechanism", cfg: KafkaConfig{SASL: KafkaSASLConfig{Mechanism: "other", Username: "test-user", Password: "synthetic-test-password"}}},
		{name: "plain", cfg: KafkaConfig{SASL: KafkaSASLConfig{Mechanism: "PLAIN", Username: "test-user", Password: "synthetic-test-password"}}, valid: true},
		{name: "scram 256", cfg: KafkaConfig{SASL: KafkaSASLConfig{Mechanism: "scram-sha-256", Username: "test-user", Password: "synthetic-test-password"}}, valid: true},
		{name: "scram 512 and TLS", cfg: KafkaConfig{TLS: KafkaTLSConfig{Enabled: true}, SASL: KafkaSASLConfig{Mechanism: "SCRAM-SHA-512", Username: "test-user", Password: "synthetic-test-password"}}, valid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKafkaSecurity(tt.cfg)
			if (err == nil) != tt.valid {
				t.Fatalf("validation error = %v, valid = %t", err, tt.valid)
			}
			if err != nil && strings.Contains(err.Error(), "synthetic-test-password") {
				t.Fatal("validation error exposed a password")
			}
		})
	}
}
