package connect

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchConnectorConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/connectors/kafka3o-restore-orders/config":
			_, _ = w.Write([]byte(`{"transforms.unwrap.restore.id":"r-1","transforms.unwrap.snapshot.boundary":"0:20"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	config, err := NewFetchConnectorConfig(server.URL, server.Client()).Execute(context.Background(), "kafka3o-restore-orders")
	if err != nil || config["transforms.unwrap.restore.id"] != "r-1" {
		t.Fatalf("config = %v, %v", config, err)
	}
	if _, err := NewFetchConnectorConfig(server.URL, server.Client()).Execute(context.Background(), "missing"); !errors.Is(err, ErrConnectorNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := NewFetchConnectorConfig("", http.DefaultClient).Execute(context.Background(), "x"); !errors.Is(err, ErrConnectNotConfigured) {
		t.Fatalf("not configured: %v", err)
	}
}
