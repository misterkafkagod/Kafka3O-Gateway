package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiddleware_CORS_PreflightAllowsHeadersAndExposesRequestID(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/topics", nil)
	req.Header.Set("Origin", "https://ui.example.com")

	var called bool
	CORS([]string{"https://ui.example.com"}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if called {
		t.Error("preflight reached the wrapped handler")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://ui.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q", got)
	}
	for _, h := range []string{HeaderAPIKey, HeaderBreakGlass, HeaderRequestID, HeaderContentType} {
		if allowed := rec.Header().Get("Access-Control-Allow-Headers"); !strings.Contains(allowed, h) {
			t.Errorf("Access-Control-Allow-Headers = %q, missing %q", allowed, h)
		}
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != HeaderRequestID {
		t.Errorf("Access-Control-Expose-Headers = %q, want %q", got, HeaderRequestID)
	}
}

func TestMiddleware_CORS_RejectsUnlistedOrigin(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/topics", nil)
	req.Header.Set("Origin", "https://evil.example.com")

	CORS([]string{"https://ui.example.com"}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("handler ran for a rejected preflight")
	})).ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty for an unlisted origin", got)
	}
}

func TestMiddleware_CORS_ActualRequestPassesThroughWithHeaders(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	req.Header.Set("Origin", "https://ui.example.com")

	var called bool
	CORS([]string{"https://ui.example.com"}, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if !called {
		t.Fatal("handler did not run for an allowed origin")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://ui.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q", got)
	}
}

func TestMiddleware_CORS_WildcardOnlyWithoutAuth(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/topics", nil)
	req.Header.Set("Origin", "https://anything.example.com")
	CORS([]string{"*"}, false)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://anything.example.com" {
		t.Errorf("auth disabled: Access-Control-Allow-Origin = %q, want the wildcard to match any origin", got)
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodOptions, "/v1/topics", nil)
	req2.Header.Set("Origin", "https://anything.example.com")
	CORS([]string{"*"}, true)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec2, req2)
	if got := rec2.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("auth enabled: Access-Control-Allow-Origin = %q, want the wildcard to be ignored (defence in depth)", got)
	}
}
