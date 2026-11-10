package s3store

import (
	"fmt"
	"strings"
)

// EncodeURIComponent applies the RFC 3986 encoding AWS Signature Version 4 requires: unreserved characters stay,
// everything else becomes %XX in upper case; slashes are encoded unless keepSlash is set.
func EncodeURIComponent(value string, keepSlash bool) string {
	var encoded strings.Builder
	for _, b := range []byte(value) {
		switch {
		case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9', b == '-', b == '_', b == '.', b == '~':
			encoded.WriteByte(b)
		case b == '/' && keepSlash:
			encoded.WriteByte(b)
		default:
			fmt.Fprintf(&encoded, "%%%02X", b)
		}
	}
	return encoded.String()
}
