package httpapi

import (
	"net/http"
	"strings"
)

func (a *SessionAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Add("Vary", "Cookie")
	if r.TLS == nil || !strings.EqualFold(r.Host, a.expectedHost) {
		a.writeProblem(w, http.StatusForbidden, "secure_origin_required", "Verified HTTPS origin required")
		return
	}
	a.mux.ServeHTTP(w, r)
}
