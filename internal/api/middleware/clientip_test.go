package middleware

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("net.ParseCIDR(%q) error: %v", s, err)
	}
	return n
}

func handlerEchoingClientIP(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(clientIPFrom(r.Context())))
	})
}

func TestMiddleware_ClientIP_TrustedProxyTakesRightmostUntrustedXFF(t *testing.T) {
	t.Parallel()
	trusted := []*net.IPNet{mustCIDR(t, "10.0.0.0/8")}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.RemoteAddr = "10.0.0.5:443" // the peer itself is a trusted proxy
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.1.2.3")

	ClientIP(trusted)(handlerEchoingClientIP(t)).ServeHTTP(rec, req)

	// Right-most untrusted entry: 10.1.2.3 is inside 10.0.0.0/8 (still a
	// trusted hop) so it is skipped; 203.0.113.9 is the first untrusted one
	// scanning from the right.
	if got := rec.Body.String(); got != "203.0.113.9" {
		t.Errorf("client IP = %q, want 203.0.113.9", got)
	}
}

func TestMiddleware_ClientIP_UntrustedPeerIgnoresXFF(t *testing.T) {
	t.Parallel()
	trusted := []*net.IPNet{mustCIDR(t, "10.0.0.0/8")}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.RemoteAddr = "198.51.100.7:12345" // not in the trusted list
	req.Header.Set("X-Forwarded-For", "203.0.113.9")

	ClientIP(trusted)(handlerEchoingClientIP(t)).ServeHTTP(rec, req)

	if got := rec.Body.String(); got != "198.51.100.7" {
		t.Errorf("client IP = %q, want the untrusted peer 198.51.100.7 (X-Forwarded-For ignored)", got)
	}
}

func TestMiddleware_ClientIP_NoTrustedProxiesConfigured(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.RemoteAddr = "192.0.2.1:80"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")

	ClientIP(nil)(handlerEchoingClientIP(t)).ServeHTTP(rec, req)

	if got := rec.Body.String(); got != "192.0.2.1" {
		t.Errorf("client IP = %q, want the peer address (default: empty trusted list)", got)
	}
}
