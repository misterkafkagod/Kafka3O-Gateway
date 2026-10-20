package httpapi

import "net/http"

type corsHandler struct {
	next    http.Handler
	allowed map[string]struct{}
}

// WithCORS lets browsers on the listed origins (exact match, no wildcard) call next.
// Requests from other origins are still served, but without CORS headers, so browsers block them.
func WithCORS(next http.Handler, allowedOrigins []string) http.Handler {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, origin := range allowedOrigins {
		allowed[origin] = struct{}{}
	}
	return corsHandler{next: next, allowed: allowed}
}

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		h.next.ServeHTTP(w, r)
		return
	}
	w.Header().Add("Vary", "Origin")
	if _, ok := h.allowed[origin]; !ok {
		h.next.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type")
		w.Header().Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	h.next.ServeHTTP(w, r)
}
