package main

import (
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func testProxy(t *testing.T, prefix, token string, handler http.Handler) (http.Handler, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewTLSServer(handler)
	t.Cleanup(upstream.Close)
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	proxy, err := newGatewayProxy(prefix, upstream.URL, token, roots)
	if err != nil {
		t.Fatal(err)
	}
	return proxy, upstream
}

func manualRequest(method, target string) *http.Request {
	request := httptest.NewRequest(method, "http://localhost:5173"+target, nil)
	request.Header.Set(proxyHeader, "1")
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	return request
}

func TestProxyUsesTheMatchingGatewayTokenAndVerifiesTLS(t *testing.T) {
	type observed struct {
		authorization, path, query, cookie, origin, marker string
	}
	requests := make(chan observed, 2)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- observed{
			authorization: r.Header.Get("Authorization"),
			path:          r.URL.EscapedPath(),
			query:         r.URL.RawQuery,
			cookie:        r.Header.Get("Cookie"),
			origin:        r.Header.Get("Origin"),
			marker:        r.Header.Get(proxyHeader),
		}
		w.Header().Set("Set-Cookie", "unwanted=value")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true}`)
	})
	proxyA, _ := testProxy(t, "/manual/gateways/a", "test-token-a", upstream)
	proxyB, _ := testProxy(t, "/manual/gateways/b", "test-token-b", upstream)
	handler := newManualHandler(t.TempDir(), map[string]http.Handler{
		"/manual/gateways/a": proxyA,
		"/manual/gateways/b": proxyB,
	})

	for _, test := range []struct {
		gateway, token string
	}{
		{"a", "test-token-a"},
		{"b", "test-token-b"},
	} {
		request := manualRequest(http.MethodGet, "/manual/gateways/"+test.gateway+"/api/v1/topics/order%20id?format=json")
		request.Header.Set("Authorization", "Bearer untrusted-client-value")
		request.Header.Set("Cookie", "unwanted=value")
		request.Header.Set("Origin", expectedOrigin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("Gateway %s: got HTTP %d: %s", test.gateway, response.Code, response.Body.String())
		}
		got := <-requests
		if got.authorization != "Bearer "+test.token || got.path != "/api/v1/topics/order%20id" || got.query != "format=json" {
			t.Errorf("Gateway %s request was not forwarded correctly: %+v", test.gateway, got)
		}
		if got.cookie != "" || got.origin != "" || got.marker != "" {
			t.Errorf("Gateway %s received client-only headers: %+v", test.gateway, got)
		}
		if response.Header().Get("Set-Cookie") != "" || strings.Contains(response.Body.String(), test.token) {
			t.Errorf("Gateway %s exposed a cookie or token to the browser", test.gateway)
		}
		if response.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("Gateway %s exposed cross-origin access to the browser", test.gateway)
		}
	}
}

func TestProxyRejectsCrossOriginAndUnmarkedRequests(t *testing.T) {
	var calls atomic.Int32
	proxy := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	})
	handler := newManualHandler(t.TempDir(), map[string]http.Handler{"/manual/gateways/a": proxy})
	for _, test := range []struct {
		name   string
		change func(*http.Request)
		status int
	}{
		{"different host", func(r *http.Request) { r.Host = "attacker.example:5173" }, http.StatusForbidden},
		{"different origin", func(r *http.Request) { r.Header.Set("Origin", "https://attacker.example") }, http.StatusForbidden},
		{"cross-site fetch", func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }, http.StatusForbidden},
		{"missing marker", func(r *http.Request) { r.Header.Del(proxyHeader) }, http.StatusForbidden},
		{"preflight", func(r *http.Request) { r.Method = http.MethodOptions }, http.StatusMethodNotAllowed},
		{"unknown method", func(r *http.Request) { r.Method = http.MethodPatch }, http.StatusMethodNotAllowed},
		{"unknown gateway", func(r *http.Request) { r.URL.Path = "/manual/gateways/b/api/v1/cluster" }, http.StatusNotFound},
		{"non-api path", func(r *http.Request) { r.URL.Path = "/manual/gateways/a/token" }, http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := manualRequest(http.MethodGet, "/manual/gateways/a/api/v1/cluster")
			test.change(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("got HTTP %d, want %d", response.Code, test.status)
			}
			if response.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatal("proxy must not allow cross-origin browser access")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected requests reached the Gateway %d times", calls.Load())
	}
}

func TestProxyPreservesWriteMethodsBodiesAndResponses(t *testing.T) {
	requests := make(chan string, 2)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		requests <- r.Method + " " + r.URL.Path + " " + string(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, `{"status":"accepted"}`)
	})
	prefix := "/manual/gateways/a"
	proxy, _ := testProxy(t, prefix, "test-token-a", upstream)
	handler := newManualHandler(t.TempDir(), map[string]http.Handler{prefix: proxy})

	for _, test := range []struct {
		method, target, body, expected string
	}{
		{http.MethodPost, "/api/v1/topics", `{"name":"example"}`, `POST /api/v1/topics {"name":"example"}`},
		{http.MethodDelete, "/api/v1/topics/example", "", "DELETE /api/v1/topics/example "},
	} {
		request := manualRequest(test.method, prefix+test.target)
		request.Body = io.NopCloser(strings.NewReader(test.body))
		request.ContentLength = int64(len(test.body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", expectedOrigin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusAccepted || response.Body.String() != `{"status":"accepted"}` {
			t.Fatalf("%s: got HTTP %d: %s", test.method, response.Code, response.Body.String())
		}
		if got := <-requests; got != test.expected {
			t.Errorf("%s: got %q, want %q", test.method, got, test.expected)
		}
	}
}

func TestProxyReportsTLSAndAuthenticationFailures(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream authentication failed", http.StatusUnauthorized)
	}))
	defer upstream.Close()
	prefix := "/manual/gateways/a"

	untrusted, err := newGatewayProxy(prefix, upstream.URL, "test-token", x509.NewCertPool())
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	trusted, err := newGatewayProxy(prefix, upstream.URL, "test-token", roots)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name, code string
		proxy      http.Handler
	}{
		{"untrusted certificate", "manual_gateway_unreachable", untrusted},
		{"rejected token", "manual_gateway_authentication_failed", trusted},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newManualHandler(t.TempDir(), map[string]http.Handler{prefix: test.proxy})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, manualRequest(http.MethodGet, prefix+"/api/v1/cluster"))
			if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), test.code) {
				t.Fatalf("got HTTP %d: %s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "test-token") {
				t.Fatal("response contains a test token")
			}
		})
	}
}

func TestManualUIServingAndHostRestrictions(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "index.html"), []byte("manual UI"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "config.json"), []byte(`{"gateways":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	handler := newManualHandler(directory, nil)
	for _, test := range []struct {
		path   string
		status int
		body   string
	}{
		{"/", http.StatusOK, "manual UI"},
		{"/clusters/kafka-a/topics", http.StatusOK, "manual UI"},
		{"/config.json", http.StatusOK, `{"gateways":[]}`},
		{"/assets/missing.js", http.StatusNotFound, ""},
		{"/manual/gateways/a/api/v1/cluster", http.StatusNotFound, ""},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, manualRequest(http.MethodGet, test.path))
		if response.Code != test.status || (test.body != "" && response.Body.String() != test.body) {
			t.Errorf("%s: got HTTP %d %q", test.path, response.Code, response.Body.String())
		}
		if response.Header().Get("Content-Security-Policy") == "" || response.Header().Get("X-Frame-Options") != "DENY" {
			t.Error("missing UI security headers")
		}
	}
}

func TestLoadCredentialsValidatesFiles(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()
	directory := t.TempDir()
	tokenPath := filepath.Join(directory, "token")
	certPath := filepath.Join(directory, "tls.crt")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw})
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("a", 64)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, certificate, 0o600); err != nil {
		t.Fatal(err)
	}
	token, roots, err := loadCredentials(directory)
	if err != nil || len(token) != 64 || len(roots.Subjects()) == 0 {
		t.Fatalf("valid test credentials were rejected: %v", err)
	}
	if err := os.WriteFile(tokenPath, []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadCredentials(directory); err == nil {
		t.Fatal("invalid token was accepted")
	}
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("a", 64)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, []byte("invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadCredentials(directory); err == nil {
		t.Fatal("invalid certificate was accepted")
	}
}

func TestProxyRejectsNonLoopbackOrInsecureTargets(t *testing.T) {
	for _, target := range []string{
		"http://127.0.0.1:8081",
		"https://example.com:8081",
		"https://127.0.0.1:8081/path",
		"https://127.0.0.1:8081?query=value",
		"invalid",
	} {
		if _, err := newGatewayProxy("/manual/gateways/a", target, "test-token", x509.NewCertPool()); err == nil {
			t.Errorf("accepted target %q", target)
		}
		if _, err := newGatewayProxy("/manual/gateways/a", "https://127.0.0.1:8081", "test-token", nil); err == nil {
			t.Fatal("proxy accepted missing certificate trust")
		}
		if _, err := newGatewayProxy("/manual/gateways/a", "https://127.0.0.1:8081", "", x509.NewCertPool()); err == nil {
			t.Fatal("proxy accepted a missing token")
		}
	}
}
