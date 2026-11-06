//go:build integration

package integration

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
)

func testKafkaSecurityRejections(t *testing.T, cfg config.KafkaConfig) {
	t.Helper()
	t.Run("bad credentials are rejected", func(t *testing.T) {
		bad := cfg
		bad.SASL.Password += "-invalid"
		badClient, err := kafka.NewClient(bad)
		if err != nil {
			t.Fatal(err)
		}
		defer badClient.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		_, err = kafka.NewDescribeCluster(badClient).Execute(ctx)
		if !errors.Is(err, kerr.SaslAuthenticationFailed) {
			t.Fatalf("incorrect credentials did not return SASL authentication failure (error type %T)", err)
		}
	})
	t.Run("untrusted certificate is rejected", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		defer server.Close()
		otherCA := filepath.Join(t.TempDir(), "other-ca.pem")
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
		if err := os.WriteFile(otherCA, pemBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		bad := cfg
		bad.TLS.CAFile = otherCA
		badClient, err := kafka.NewClient(bad)
		if err != nil {
			t.Fatal(err)
		}
		defer badClient.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		_, err = kafka.NewDescribeCluster(badClient).Execute(ctx)
		var unknownAuthority x509.UnknownAuthorityError
		if !errors.As(err, &unknownAuthority) {
			t.Fatalf("untrusted certificate did not return an unknown-authority error (error type %T)", err)
		}
	})
}
