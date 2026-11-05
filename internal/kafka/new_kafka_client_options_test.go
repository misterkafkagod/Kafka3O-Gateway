package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func TestNewKafkaClientOptions_SecurityModes(t *testing.T) {
	tests := []struct {
		name      string
		tls       bool
		mechanism string
	}{
		{name: "plaintext"},
		{name: "TLS with system roots", tls: true},
		{name: "PLAIN", mechanism: "PLAIN"},
		{name: "SCRAM-SHA-256", mechanism: "scram-sha-256"},
		{name: "TLS and SCRAM-SHA-512", tls: true, mechanism: "SCRAM-SHA-512"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.KafkaConfig{Brokers: []string{"127.0.0.1:1"}, TLS: config.KafkaTLSConfig{Enabled: tt.tls}}
			if tt.mechanism != "" {
				cfg.SASL = config.KafkaSASLConfig{Mechanism: tt.mechanism, Username: "test-user", Password: "synthetic-test-password"}
			}
			opts, err := newKafkaClientOptions(cfg, func(string) ([]byte, error) {
				t.Fatal("unexpected CA file read")
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			client, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if enabled := client.OptValue(kgo.DialTLS).(bool); enabled != tt.tls {
				t.Fatalf("TLS enabled = %t, want %t", enabled, tt.tls)
			}
			if tt.tls {
				got := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
				if got.MinVersion != tls.VersionTLS12 || got.RootCAs != nil || got.InsecureSkipVerify {
					t.Fatal("unexpected system-root TLS options")
				}
			}
			mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
			if tt.mechanism == "" {
				if len(mechanisms) != 0 {
					t.Fatal("unexpected SASL mechanism")
				}
			} else if len(mechanisms) != 1 || mechanisms[0].Name() != strings.ToUpper(tt.mechanism) {
				t.Fatal("SASL mechanism was not configured")
			}
		})
	}
}

func TestKafkaClients_UseCustomCAAndSASL(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, cert, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.KafkaConfig{
		Brokers: []string{"127.0.0.1:1"},
		TLS:     config.KafkaTLSConfig{Enabled: true, CAFile: path},
		SASL:    config.KafkaSASLConfig{Mechanism: "SCRAM-SHA-512", Username: "test-user", Password: "synthetic-test-password"},
	}
	for name, create := range map[string]func(config.KafkaConfig) (*kgo.Client, error){"client": NewClient, "producer": NewProducer} {
		t.Run(name, func(t *testing.T) {
			client, err := create(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			tlsConfig := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
			if tlsConfig == nil || tlsConfig.RootCAs == nil || len(tlsConfig.RootCAs.Subjects()) != 1 ||
				tlsConfig.MinVersion != tls.VersionTLS12 || tlsConfig.InsecureSkipVerify {
				t.Fatal("custom CA was not configured securely")
			}
			mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
			if len(mechanisms) != 1 || mechanisms[0].Name() != "SCRAM-SHA-512" {
				t.Fatal("SCRAM was not configured")
			}
		})
	}
}

func TestNewKafkaClientOptions_RejectsInvalidCA(t *testing.T) {
	cfg := config.KafkaConfig{TLS: config.KafkaTLSConfig{Enabled: true, CAFile: "ca.pem"}}
	want := errors.New("read failure")
	if _, err := newKafkaClientOptions(cfg, func(string) ([]byte, error) { return nil, want }); !errors.Is(err, want) {
		t.Fatalf("CA file error = %v", err)
	}
	if _, err := newKafkaClientOptions(cfg, func(string) ([]byte, error) { return []byte("not a certificate"), nil }); err == nil {
		t.Fatal("expected an invalid CA error")
	}
}

func TestKafkaClient_RejectsBrokerHostnameMismatch(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              []string{"broker.example"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	defer server.Close()

	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(config.KafkaConfig{
		Brokers: []string{server.Listener.Addr().String()},
		TLS:     config.KafkaTLSConfig{Enabled: true, CAFile: path},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	dial := client.OptValue(kgo.Dialer).(func(context.Context, string, string) (net.Conn, error))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dial(ctx, "tcp", server.Listener.Addr().String())
	if conn != nil {
		defer conn.Close()
	}
	var hostnameError x509.HostnameError
	if !errors.As(err, &hostnameError) {
		t.Fatalf("mismatched broker hostname did not return a hostname error (error type %T)", err)
	}
}
