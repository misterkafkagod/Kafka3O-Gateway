package delegation

import "encoding/base64"

// validHandle accepts exactly 32 bytes of unpadded base64url.
func validHandle(handle string) bool {
	if len(handle) != 43 {
		return false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(handle)
	return err == nil && len(raw) == 32
}
