package app

import (
	"net/http"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/config"
)

func TestApp_ServerTimeoutsFromConfig(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.HTTP.Addr = ":0"
	cfg.Kafka.Bootstrap = []string{"localhost:9092"}
	cfg.Auth.Enabled = false
	if err := config.Validate(&cfg); err != nil {
		t.Fatalf("config.Validate() error: %v", err)
	}

	server, err := newServer(cfg.HTTP, http.NotFoundHandler())
	if err != nil {
		t.Fatalf("newServer() error: %v", err)
	}

	if server.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", server.ReadHeaderTimeout)
	}
	if server.IdleTimeout != 60*time.Second {
		t.Errorf("IdleTimeout = %v, want 60s", server.IdleTimeout)
	}
	wantWrite := cfg.Bounds.Scan.MaxTime.Ceiling + 5*time.Second
	if server.WriteTimeout != wantWrite {
		t.Errorf("WriteTimeout = %v, want ceiling+5s = %v", server.WriteTimeout, wantWrite)
	}
}
