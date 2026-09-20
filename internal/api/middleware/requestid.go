// Package middleware is the gateway's net/http middleware chain (TECH-SPEC
// §5.1 "HTTP-only gates"): request id, client IP resolution, API-key
// authentication, and CORS — everything FUNC-SPEC §9.1 nodes B through D
// that runs before Huma ever sees a request. Command-level policy gates
// (F-K3) live in internal/service/gates, not here (TECH-SPEC P1).
package middleware

import (
	"net/http"
	"regexp"

	"github.com/misterkafkagod/kafka3o/internal/api/errors"
)

// Header names the middleware chain reads or sets (FUNC-SPEC §8.2).
const (
	HeaderRequestID = "X-Request-Id"
	// HeaderAPIKey is a header *name*, not a credential value.
	//nolint:gosec // G101: false positive — this is the header name "X-Api-Key", not a hardcoded secret.
	HeaderAPIKey      = "X-Api-Key"
	HeaderBreakGlass  = "X-Break-Glass-Reason"
	HeaderContentType = "Content-Type"
)

// requestIDPattern is the exact echo rule (TECH-SPEC C2, FUNC-SPEC X4): a
// supplied id outside this shape is replaced, never echoed.
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// RequestID echoes a valid X-Request-Id, generates one otherwise, sets it on
// the response header, and stores it in the request context for every
// downstream handler and error constructor to read via errors.RequestIDFrom
// (FUNC-SPEC X4; TECH-SPEC C2).
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if !requestIDPattern.MatchString(id) {
			id = randomRequestID()
		}
		w.Header().Set(HeaderRequestID, id)
		next.ServeHTTP(w, r.WithContext(errors.SetRequestID(r.Context(), id)))
	})
}
