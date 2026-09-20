package middleware

import (
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func testKeys() []Key {
	return []Key{
		{ID: "ops", Tier: core.TierOperator, SHA256: sha256.Sum256([]byte("operator-secret"))},
		{ID: "ui", Tier: core.TierReader, SHA256: sha256.Sum256([]byte("reader-secret"))},
	}
}

func handlerCapturingCaller(t *testing.T, got *core.Caller, ok *bool) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got, *ok = CallerFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})
}

func TestMiddleware_APIKey_MissingIs401(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)

	var caller core.Caller
	var called bool
	APIKey(testKeys(), true)(handlerCapturingCaller(t, &caller, &called)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("handler ran despite a missing key")
	}
	assertUnauthenticatedBody(t, rec)
}

func TestMiddleware_APIKey_UnknownIs401(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	req.Header.Set(HeaderAPIKey, "not-a-configured-key")

	var caller core.Caller
	var called bool
	APIKey(testKeys(), true)(handlerCapturingCaller(t, &caller, &called)).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("handler ran despite an unknown key")
	}
}

func TestMiddleware_APIKey_ValidBuildsCallerWithTier(t *testing.T) {
	t.Parallel()
	cases := []struct {
		key, wantID, wantTier string
	}{
		{"operator-secret", "ops", core.TierOperator},
		{"reader-secret", "ui", core.TierReader},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
		req.Header.Set(HeaderAPIKey, tc.key)
		req.Header.Set(HeaderBreakGlass, "incident-1")

		var caller core.Caller
		var called bool
		APIKey(testKeys(), true)(handlerCapturingCaller(t, &caller, &called)).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK || !called {
			t.Fatalf("key %q: status=%d called=%v, want 200/true", tc.key, rec.Code, called)
		}
		if caller.KeyID == nil || *caller.KeyID != tc.wantID {
			t.Errorf("key %q: KeyID = %v, want %q", tc.key, caller.KeyID, tc.wantID)
		}
		if caller.Tier != tc.wantTier {
			t.Errorf("key %q: Tier = %q, want %q", tc.key, caller.Tier, tc.wantTier)
		}
		if caller.BreakGlassReason != "incident-1" {
			t.Errorf("key %q: BreakGlassReason = %q, want incident-1", tc.key, caller.BreakGlassReason)
		}
	}
}

func TestMiddleware_APIKey_AuthDisabledIsOperatorNilKeyID(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	// No X-Api-Key at all: auth is disabled, so this must still succeed.

	var caller core.Caller
	var called bool
	APIKey(nil, false)(handlerCapturingCaller(t, &caller, &called)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || !called {
		t.Fatalf("status=%d called=%v, want 200/true", rec.Code, called)
	}
	if caller.KeyID != nil {
		t.Errorf("KeyID = %v, want nil", caller.KeyID)
	}
	if caller.Tier != core.TierOperator {
		t.Errorf("Tier = %q, want operator", caller.Tier)
	}
}

func TestMiddleware_APIKey_HealthAndOpenAPINotExempt(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/health/live", "/health/ready", "/openapi.json", "/docs"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)

		var caller core.Caller
		var called bool
		APIKey(testKeys(), true)(handlerCapturingCaller(t, &caller, &called)).ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("path %s: status = %d, want 401 (no endpoint is exempt)", path, rec.Code)
		}
		if called {
			t.Errorf("path %s: handler ran without a key", path)
		}
	}
}

func TestMiddleware_APIKey_PreflightBypassesAuth(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/v1/topics", nil)

	var called bool
	APIKey(testKeys(), true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})).ServeHTTP(rec, req)

	if !called {
		t.Error("OPTIONS preflight was blocked by api-key auth")
	}
}

func TestSanitizeBreakGlass(t *testing.T) {
	t.Parallel()
	if got := sanitizeBreakGlass("incident\x00\x1f\x7f-42\r\n"); got != "incident-42" {
		t.Errorf("sanitizeBreakGlass() = %q, want control characters stripped", got)
	}
	long := strings.Repeat("é", 400) // 2 bytes each = 800 bytes raw
	got := sanitizeBreakGlass(long)
	if len(got) > breakGlassMaxBytes {
		t.Errorf("sanitizeBreakGlass() = %d bytes, want <= %d", len(got), breakGlassMaxBytes)
	}
	if !utf8.ValidString(got) {
		t.Errorf("sanitizeBreakGlass() produced invalid UTF-8 at the truncation boundary: %q", got)
	}
}

func assertUnauthenticatedBody(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not JSON: %v", err)
	}
	errObj, ok := body["error"].(map[string]any)
	if !ok || errObj["code"] != "UNAUTHENTICATED" {
		t.Errorf("body = %s, want error.code = UNAUTHENTICATED", rec.Body.String())
	}
}
