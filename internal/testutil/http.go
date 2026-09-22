package testutil

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
)

// Do issues a request against the running gateway with the given method,
// path, and optional JSON body, presenting DefaultOperatorKey (FUNC-SPEC
// B3: no route is exempt from authentication, including /health/* and
// /openapi.json).
func (gw *Gateway) Do(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()
	return gw.DoWithKey(t, method, path, body, DefaultOperatorKey)
}

// DoWithKey is Do, presenting apiKey instead of DefaultOperatorKey — for
// tests exercising authentication itself (e.g. a key WithKeys did not
// configure).
func (gw *Gateway) DoWithKey(t *testing.T, method, path string, body any, apiKey string) *http.Response {
	t.Helper()
	return gw.DoWithHeaders(t, method, path, body, apiKey, nil)
}

// DoWithHeaders is DoWithKey, also setting every header in extra (e.g.
// X-Break-Glass-Reason) — for tests exercising request headers beyond the
// API key itself.
func (gw *Gateway) DoWithHeaders(t *testing.T, method, path string, body any, apiKey string, extra map[string]string) *http.Response {
	t.Helper()

	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("json.Marshal(body) error: %v", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, gw.Server.URL+path, reader)
	if err != nil {
		t.Fatalf("http.NewRequest(%s %q) error: %v", method, path, err)
	}
	if body != nil {
		req.Header.Set(middleware.HeaderContentType, "application/json")
	}
	if apiKey != "" {
		req.Header.Set(middleware.HeaderAPIKey, apiKey)
	}
	for name, value := range extra {
		req.Header.Set(name, value)
	}

	resp, err := gw.Server.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s error: %v", method, path, err)
	}
	return resp
}

// Get issues a GET request (see Do).
func (gw *Gateway) Get(t *testing.T, path string) *http.Response {
	t.Helper()
	return gw.Do(t, http.MethodGet, path, nil)
}
