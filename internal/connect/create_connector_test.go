package connect

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateConnectorPostsTheConfiguration(t *testing.T) {
	var got struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/connectors" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request %s %s %s", r.Method, r.URL.Path, r.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	if err := NewCreateConnector(server.URL+"/", server.Client()).Execute(context.Background(), "kafka3o-backup-orders", map[string]string{"topics": "orders"}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "kafka3o-backup-orders" || got.Config["topics"] != "orders" {
		t.Fatalf("posted %+v", got)
	}
}

func TestCreateConnectorMapsFailures(t *testing.T) {
	for _, tt := range []struct {
		status int
		want   error
	}{
		{http.StatusConflict, ErrConnectorExists},
		{http.StatusBadRequest, ErrConnectRejected},
		{http.StatusInternalServerError, ErrConnectUnreachable},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.status) }))
		err := NewCreateConnector(server.URL, server.Client()).Execute(context.Background(), "sink", map[string]string{})
		server.Close()
		if !errors.Is(err, tt.want) {
			t.Errorf("status %d: %v", tt.status, err)
		}
	}
	if err := NewCreateConnector("", http.DefaultClient).Execute(context.Background(), "sink", nil); !errors.Is(err, ErrConnectNotConfigured) {
		t.Errorf("not configured: %v", err)
	}
	if err := NewCreateConnector("http://127.0.0.1:1", http.DefaultClient).Execute(context.Background(), "a/b", nil); !errors.Is(err, ErrInvalidConnectorName) {
		t.Errorf("invalid name: %v", err)
	}
	if err := NewCreateConnector("http://127.0.0.1:1", http.DefaultClient).Execute(context.Background(), "sink", nil); !errors.Is(err, ErrConnectActionUnknown) {
		t.Errorf("no response: %v", err)
	}
}
