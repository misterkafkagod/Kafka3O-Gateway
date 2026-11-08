package identity

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// validChangeReason accepts a trimmed, printable reason of 1–512 bytes.
func validChangeReason(reason string) bool {
	return reason != "" && len(reason) <= 512 && utf8.ValidString(reason) &&
		strings.TrimSpace(reason) == reason && strings.IndexFunc(reason, unicode.IsControl) < 0
}
