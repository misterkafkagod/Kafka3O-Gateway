package s3store

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGetObjectStreamsASignedRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/kafka3o-backups/kafka-a/orders/orders-0-00000000000000000000.gz" ||
			!strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=read-key/") {
			t.Errorf("request %s %v", r.URL.EscapedPath(), r.Header)
		}
		_, _ = w.Write([]byte("object body"))
	}))
	defer server.Close()
	body, err := NewGetObject(server.URL, "kafka3o-backups", "us-east-1", Credentials{AccessKey: "read-key", SecretKey: "s"}, server.Client(), time.Now).
		Execute(context.Background(), "kafka-a/orders/orders-0-00000000000000000000.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if data, _ := io.ReadAll(body); string(data) != "object body" {
		t.Fatalf("body = %q", data)
	}
}

func TestGetObjectFailures(t *testing.T) {
	if _, err := NewGetObject("http://s3", "b", "us-east-1", Credentials{}, http.DefaultClient, time.Now).Execute(context.Background(), "k"); !errors.Is(err, ErrNoCredentials) {
		t.Errorf("no credentials: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer server.Close()
	if _, err := NewGetObject(server.URL, "b", "us-east-1", Credentials{AccessKey: "a", SecretKey: "s"}, server.Client(), time.Now).Execute(context.Background(), "k"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("missing object: %v", err)
	}
}
