package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFrontRelaysOnlyTheForwardedOriginAndServesTheUI(t *testing.T) {
	var seen *http.Request
	console := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Clone(r.Context())
		if r.URL.Path == "/api/redirect" {
			http.Redirect(w, r, "https://elsewhere.example/", http.StatusFound)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "__Host-kafka3o_session", Value: "s", Secure: true, HttpOnly: true, Path: "/"})
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer console.Close()
	roots := console.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	relay, err := newConsoleRelay(console.URL, roots)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	_ = os.WriteFile(filepath.Join(directory, "index.html"), []byte("<html>ui</html>"), 0o600)
	_ = os.MkdirAll(filepath.Join(directory, "assets"), 0o700)
	_ = os.WriteFile(filepath.Join(directory, "assets", "app.js"), []byte("js"), 0o600)
	handler := newFrontHandler(directory, relay)
	do := func(method, path, host, origin string, headers ...string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, "http://"+host+path, strings.NewReader(`{"x":1}`))
		request.Host = host
		if origin != "" {
			request.Header.Set("Origin", origin)
		}
		for i := 0; i+1 < len(headers); i += 2 {
			request.Header.Set(headers[i], headers[i+1])
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder
	}
	response := do("POST", "/api/session/local", "localhost:5173", "http://localhost:5173", "X-Forwarded-For", "1.2.3.4", "Cookie", "a=b")
	if response.Code != 200 || seen.Header.Get("Origin") != console.URL || seen.Host != strings.TrimPrefix(console.URL, "https://") ||
		seen.Header.Get("X-Forwarded-For") != "" || seen.Header.Get("Cookie") != "a=b" || !strings.Contains(response.Header().Get("Set-Cookie"), "__Host-kafka3o_session") {
		t.Fatalf("relay = %d origin=%q host=%q xff=%q", response.Code, seen.Header.Get("Origin"), seen.Host, seen.Header.Get("X-Forwarded-For"))
	}
	if response := do("GET", "/api/session", "localhost:5173", ""); response.Code != 200 || seen.Header.Get("Origin") != "" {
		t.Fatalf("same-origin GET without Origin = %d origin=%q", response.Code, seen.Header.Get("Origin"))
	}
	for _, tt := range []struct {
		host, origin string
		status       int
	}{
		{"127.0.0.1:5173", "", http.StatusMisdirectedRequest}, {"attacker.example", "", http.StatusMisdirectedRequest},
		{"localhost:5173", "https://attacker.example", http.StatusForbidden}, {"localhost:5173", "http://localhost:5174", http.StatusForbidden},
	} {
		seen = nil
		if response := do("POST", "/api/session/local", tt.host, tt.origin); response.Code != tt.status || seen != nil {
			t.Fatalf("host=%s origin=%s status=%d relayed=%t", tt.host, tt.origin, response.Code, seen != nil)
		}
	}
	if response := do("GET", "/api/redirect", "localhost:5173", ""); response.Code != http.StatusBadGateway {
		t.Fatalf("redirecting Console = %d", response.Code)
	}
	if response := do("GET", "/clusters/kafka-b/topics", "localhost:5173", ""); response.Code != 200 || response.Body.String() != "<html>ui</html>" ||
		!strings.Contains(response.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Fatalf("SPA fallback = %d %q", response.Code, response.Body.String())
	}
	if response := do("GET", "/assets/app.js", "localhost:5173", ""); response.Code != 200 || response.Body.String() != "js" {
		t.Fatalf("asset = %d", response.Code)
	}
	if response := do("POST", "/clusters", "localhost:5173", ""); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("non-API POST = %d", response.Code)
	}
	if response := do("GET", "/../../etc/passwd", "localhost:5173", ""); strings.Contains(response.Body.String(), "root:") || response.Code == 200 && response.Body.String() != "<html>ui</html>" {
		t.Fatalf("path traversal served %d %q", response.Code, response.Body.String())
	}
	if _, err := newConsoleRelay("http://127.0.0.1:8443", roots); err == nil {
		t.Fatal("a plain HTTP Console origin was accepted")
	}
	untrusted, _ := newConsoleRelay(console.URL, nil)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "http://localhost:5173/api/session", nil)
	request.Host = "localhost:5173"
	newFrontHandler(directory, untrusted).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("a Console with an unverifiable certificate was trusted: %d", recorder.Code)
	}
}
