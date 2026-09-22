package audit

import (
	"crypto/rand"
	"encoding/hex"
)

// NewEventID returns a fresh 16-byte hex-encoded id (32 characters) for an
// Event's EventID field — the same construction as the request-id
// middleware's randomRequestID, independently defined here since internal/api
// may not import internal/audit (TECH-SPEC §5.3) and vice versa.
func NewEventID() string {
	b := make([]byte, 16)
	// crypto/rand.Read on the standard reader never returns a short read or
	// an error in practice (Go's runtime source is always available); a
	// failure here would mean the OS entropy source is broken, which no
	// fallback could meaningfully recover from.
	if _, err := rand.Read(b); err != nil {
		panic("audit: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}
