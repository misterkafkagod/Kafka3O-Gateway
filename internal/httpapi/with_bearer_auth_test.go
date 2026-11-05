package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWithBearerAuth_ProtectsAllExceptLiveness(t *testing.T) {
	token := strings.Repeat("a", 32)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := WithBearerAuth(next, token)
	tests := []struct {
		name          string
		method        string
		path          string
		authorization []string
		want          int
	}{
		{name: "liveness", method: http.MethodGet, path: "/api/v1/health/live", want: http.StatusNoContent},
		{name: "liveness post", method: http.MethodPost, path: "/api/v1/health/live", want: http.StatusUnauthorized},
		{name: "missing", method: http.MethodGet, path: "/api/v1/topics", want: http.StatusUnauthorized},
		{name: "invalid", method: http.MethodGet, path: "/api/v1/topics", authorization: []string{"Bearer wrong"}, want: http.StatusUnauthorized},
		{name: "wrong scheme", method: http.MethodGet, path: "/api/v1/topics", authorization: []string{"Basic " + token}, want: http.StatusUnauthorized},
		{name: "multiple", method: http.MethodGet, path: "/api/v1/topics", authorization: []string{"Bearer " + token, "Bearer wrong"}, want: http.StatusUnauthorized},
		{name: "valid", method: http.MethodGet, path: "/api/v1/topics", authorization: []string{"Bearer " + token}, want: http.StatusNoContent},
		{name: "case insensitive scheme", method: http.MethodPost, path: "/api/v1/topics", authorization: []string{"bearer " + token}, want: http.StatusNoContent},
		{name: "protected docs", method: http.MethodGet, path: "/openapi.json", want: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			for _, value := range tt.authorization {
				req.Header.Add("Authorization", value)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.want || rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("status = %d, headers = %v", rec.Code, rec.Header())
			}
			if strings.Contains(rec.Body.String(), token) {
				t.Error("response disclosed the token")
			}
			if tt.want == http.StatusUnauthorized {
				if rec.Header().Get("WWW-Authenticate") != `Bearer realm="Kafka3O"` ||
					!strings.Contains(rec.Body.String(), `"code":"unauthorized"`) {
					t.Errorf("authentication response = %v %s", rec.Header(), rec.Body.String())
				}
			}
		})
	}
}

func TestWithBearerAuth_AllowedCORSPreflightDoesNotRequireToken(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := WithCORS(WithBearerAuth(next, strings.Repeat("a", 32)), []string{uiOrigin})
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/topics", nil)
	req.Header.Set("Origin", uiOrigin)
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "authorization,content-type")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || rec.Header().Get("Access-Control-Allow-Origin") != uiOrigin ||
		!strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Errorf("preflight = %d %v", rec.Code, rec.Header())
	}
}
