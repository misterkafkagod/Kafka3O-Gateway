package s3store

import (
	"crypto/hmac"
	"crypto/sha256"
)

// HMACSHA256 is one step of the Signature Version 4 signing-key derivation.
func HMACSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
