package s3store

import (
	"encoding/hex"
	"testing"
)

func TestHMACSHA256(t *testing.T) {
	// RFC 4231 test case 2.
	if got := hex.EncodeToString(HMACSHA256([]byte("Jefe"), "what do ya want for nothing?")); got != "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843" {
		t.Fatalf("hmac = %s", got)
	}
}
