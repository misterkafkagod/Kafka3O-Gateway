package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/api/errors"
)

func handlerEchoingRequestID(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(errors.RequestIDFrom(r.Context())))
	})
}

func TestMiddleware_RequestID_EchoesValid(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(HeaderRequestID, "abc-123.DEF_456")

	RequestID(handlerEchoingRequestID(t)).ServeHTTP(rec, req)

	if got := rec.Header().Get(HeaderRequestID); got != "abc-123.DEF_456" {
		t.Errorf("response header = %q, want the echoed id", got)
	}
	if rec.Body.String() != "abc-123.DEF_456" {
		t.Errorf("context id = %q, want the echoed id", rec.Body.String())
	}
}

func TestMiddleware_RequestID_GeneratesForInvalid(t *testing.T) {
	t.Parallel()
	cases := []string{"", "has spaces", "has/slash", string(make([]byte, 200))}
	for _, in := range cases {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		if in != "" {
			req.Header.Set(HeaderRequestID, in)
		}

		RequestID(handlerEchoingRequestID(t)).ServeHTTP(rec, req)

		got := rec.Header().Get(HeaderRequestID)
		if got == "" || got == in {
			t.Errorf("input %q: response header = %q, want a freshly generated id", in, got)
		}
		if rec.Body.String() != got {
			t.Errorf("input %q: context id %q != response header %q", in, rec.Body.String(), got)
		}
	}
}

func TestMiddleware_RequestID_PresentOnErrors(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)

	errHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	RequestID(errHandler).ServeHTTP(rec, req)

	if rec.Header().Get(HeaderRequestID) == "" {
		t.Error("X-Request-Id missing on an error response")
	}
}
