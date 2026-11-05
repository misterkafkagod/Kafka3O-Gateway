//go:build integration

package integration

import "testing"

func TestValidateExternalTestBroker(t *testing.T) {
	tests := []struct {
		name       string
		address    string
		disposable string
		valid      bool
	}{
		{name: "IPv4 loopback", address: "127.0.0.1:19092", disposable: "true", valid: true},
		{name: "IPv6 loopback", address: "[::1]:9092", disposable: "true", valid: true},
		{name: "missing acknowledgement", address: "127.0.0.1:19092"},
		{name: "incorrect acknowledgement", address: "127.0.0.1:19092", disposable: "yes"},
		{name: "empty address", disposable: "true"},
		{name: "hostname", address: "localhost:19092", disposable: "true"},
		{name: "remote IP", address: "192.168.1.109:9092", disposable: "true"},
		{name: "wildcard IP", address: "0.0.0.0:9092", disposable: "true"},
		{name: "missing port", address: "127.0.0.1", disposable: "true"},
		{name: "zero port", address: "127.0.0.1:0", disposable: "true"},
		{name: "invalid port", address: "127.0.0.1:65536", disposable: "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExternalTestBroker(tt.address, tt.disposable)
			if (err == nil) != tt.valid {
				t.Fatalf("ValidateExternalTestBroker(%q, %q) error = %v, valid = %t", tt.address, tt.disposable, err, tt.valid)
			}
		})
	}
}
