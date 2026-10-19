package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

const uiOrigin = "http://localhost:5173"

func serveWithCORS(t *testing.T, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	WithCORS(next, []string{uiOrigin}).ServeHTTP(rec, req)
	return rec
}

func TestWithCORS_AllowedOriginGetsHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	req.Header.Set("Origin", uiOrigin)

	rec := serveWithCORS(t, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != uiOrigin {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, uiOrigin)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want Origin", got)
	}
}

func TestWithCORS_AllowedPreflightReturns204(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/cluster", nil)
	req.Header.Set("Origin", uiOrigin)
	req.Header.Set("Access-Control-Request-Method", "GET")

	rec := serveWithCORS(t, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET" {
		t.Errorf("Access-Control-Allow-Methods = %q, want GET", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "Accept, Content-Type" {
		t.Errorf("Access-Control-Allow-Headers = %q, want Accept, Content-Type", got)
	}
}

func TestWithCORS_UnlistedOriginGetsNoHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	req.Header.Set("Origin", "http://evil.example")

	rec := serveWithCORS(t, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (request still served)", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want none", got)
	}
}

func TestWithCORS_NoOriginGetsNoHeaders(t *testing.T) {
	rec := serveWithCORS(t, httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil))

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want none", got)
	}
	if got := rec.Header().Get("Vary"); got != "" {
		t.Errorf("Vary = %q, want none", got)
	}
}
