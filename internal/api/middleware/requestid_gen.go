package middleware

import (
	"crypto/rand"
	"encoding/hex"
)

// randomRequestID returns a 16-byte hex-encoded id (32 characters), well
// within the ^[A-Za-z0-9._-]{1,128}$ pattern it will always be echoed under
// on a later hop.
func randomRequestID() string {
	b := make([]byte, 16)
	// crypto/rand.Read on the standard reader never returns a short read or
	// an error in practice (Go's runtime source is always available); a
	// failure here would mean the OS entropy source is broken, which no
	// fallback could meaningfully recover from.
	if _, err := rand.Read(b); err != nil {
		panic("middleware: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}
