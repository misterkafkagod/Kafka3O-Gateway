package config

import (
	"crypto/tls"
	"errors"
	"strings"
	"testing"
)

func TestLoadGatewaySecurity_ValidatesAndLoadsCertificate(t *testing.T) {
	token := strings.Repeat("a", 32)
	config := TLSConfig{CertificateFile: "certificate.pem", KeyFile: "private-key.pem"}
	called := false
	loader := func(certFile, keyFile string) (tls.Certificate, error) {
		called = true
		if certFile != config.CertificateFile || keyFile != config.KeyFile {
			t.Errorf("certificate paths = %q, %q", certFile, keyFile)
		}
		return tls.Certificate{Certificate: [][]byte{{1, 2, 3}}}, nil
	}
	got, err := LoadGatewaySecurity(config, token, loader)
	if err != nil || !called || len(got.Certificate) != 1 {
		t.Errorf("certificate = %#v, called = %v, error = %v", got, called, err)
	}
}

func TestLoadGatewaySecurity_RejectsInvalidConfigurationWithoutLeakingToken(t *testing.T) {
	valid := TLSConfig{CertificateFile: "certificate.pem", KeyFile: "private-key.pem"}
	failure := errors.New("invalid certificate")
	loader := func(string, string) (tls.Certificate, error) { return tls.Certificate{}, failure }
	for _, token := range []string{"", "short-secret", strings.Repeat("a", 32) + " invalid"} {
		_, err := LoadGatewaySecurity(valid, token, loader)
		if err == nil || (token != "" && strings.Contains(err.Error(), token)) {
			t.Errorf("invalid token error = %v", err)
		}
	}
	if _, err := LoadGatewaySecurity(TLSConfig{}, strings.Repeat("a", 32), loader); err == nil {
		t.Error("missing certificate paths must fail")
	}
	if _, err := LoadGatewaySecurity(valid, strings.Repeat("a", 32), loader); !errors.Is(err, failure) {
		t.Errorf("certificate error = %v, want %v", err, failure)
	}
}
