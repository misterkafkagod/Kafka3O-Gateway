package middleware

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP resolves the caller's address (TECH-SPEC B6): when the peer is
// inside one of trustedProxies, the right-most untrusted entry of
// X-Forwarded-For is used (the entry the trusted proxy itself appended is
// the one closest to it — the right-most — so a spoofed left-most entry from
// the original client is never trusted); otherwise the TCP peer address is
// used and X-Forwarded-For is ignored entirely.
func ClientIP(trustedProxies []*net.IPNet) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := resolveClientIP(r, trustedProxies)
			next.ServeHTTP(w, r.WithContext(withClientIP(r.Context(), ip)))
		})
	}
}

func resolveClientIP(r *http.Request, trustedProxies []*net.IPNet) string {
	peer := peerIP(r.RemoteAddr)
	if !isTrusted(peer, trustedProxies) {
		return peer
	}

	xff := r.Header.Get("X-Forwarded-For")
	entries := strings.Split(xff, ",")
	for i := len(entries) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(entries[i])
		if candidate == "" {
			continue
		}
		if !isTrusted(candidate, trustedProxies) {
			return candidate
		}
	}
	return peer
}

// peerIP strips the port from r.RemoteAddr, falling back to the raw value if
// it isn't in host:port form (e.g. in unit tests).
func peerIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func isTrusted(ip string, trustedProxies []*net.IPNet) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range trustedProxies {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}
