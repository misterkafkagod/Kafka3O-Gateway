package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"net/http"
	"strings"
)

type bearerAuthHandler struct {
	next     http.Handler
	expected [sha256.Size]byte
}

func WithBearerAuth(next http.Handler, token string) http.Handler {
	return bearerAuthHandler{next: next, expected: sha256.Sum256([]byte(token))}
}

func (h bearerAuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodGet && r.URL.Path == "/api/v1/health/live" {
		h.next.ServeHTTP(w, r)
		return
	}
	headers := r.Header.Values("Authorization")
	if len(headers) == 1 {
		fields := strings.Fields(headers[0])
		if len(fields) == 2 && strings.EqualFold(fields[0], "Bearer") {
			got := sha256.Sum256([]byte(fields[1]))
			if subtle.ConstantTimeCompare(got[:], h.expected[:]) == 1 {
				h.next.ServeHTTP(w, r)
				return
			}
		}
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("WWW-Authenticate", `Bearer realm="Kafka3O"`)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = io.WriteString(w, `{"title":"Authentication required","status":401,"code":"unauthorized"}`+"\n")
}
