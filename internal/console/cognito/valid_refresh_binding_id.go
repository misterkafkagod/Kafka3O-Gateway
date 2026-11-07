package cognito

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func validRefreshBindingID(id string) bool {
	return id != "" && len(id) <= 128 && utf8.ValidString(id) &&
		strings.TrimSpace(id) == id && strings.IndexFunc(id, unicode.IsControl) < 0
}
