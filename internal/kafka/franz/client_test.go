package franz

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

var _ kafka.Admin = (*Client)(nil)

func TestFranz_New_BuildsWithoutDialing(t *testing.T) {
	t.Parallel()
	c, err := New(Config{Bootstrap: []string{"127.0.0.1:0"}})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	c.Close()
}

func TestFranz_New_PropagatesTLSAndSASLErrors(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{
		Bootstrap: []string{"127.0.0.1:0"},
		TLS:       TLSConfig{Enabled: true, CAFile: "does-not-exist.pem"},
	}); err == nil {
		t.Error("New() with a bad CA file returned nil error")
	}
	if _, err := New(Config{
		Bootstrap: []string{"127.0.0.1:0"},
		SASL:      SASLConfig{Mechanism: "GSSAPI"},
	}); err == nil {
		t.Error("New() with an unknown SASL mechanism returned nil error")
	}
}
