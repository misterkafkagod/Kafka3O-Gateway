package middleware

import (
	"net/http"
	"slices"
	"strings"
)

// CORS allows the configured origins to call the gateway from a browser
// (FUNC-SPEC X5): it echoes back one matching origin, allows the four
// gateway-specific headers, and exposes X-Request-Id so a browser client can
// read it (TECH-SPEC C13). A wildcard origin is only honoured when
// authEnabled is false — internal/config.Validate already rejects a
// wildcard while auth is enabled at start-up, so this is defence in depth.
func CORS(origins []string, authEnabled bool) func(http.Handler) http.Handler {
	// corsAllowedHeaders are the request headers CORS preflight permits
	// (TECH-SPEC C13).
	corsAllowedHeaders := strings.Join([]string{HeaderAPIKey, HeaderBreakGlass, HeaderRequestID, HeaderContentType}, ", ")

	wildcard := !authEnabled && slices.Contains(origins, "*")
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[o] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" || (!wildcard && !allowed[origin]) {
				if r.Method == http.MethodOptions {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Expose-Headers", HeaderRequestID)

			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", corsAllowedHeaders)
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
