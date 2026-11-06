package cognito

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func validAuthorizationCode(code string) bool {
	return code != "" && len(code) <= 2048 && utf8.ValidString(code) &&
		strings.IndexFunc(code, unicode.IsControl) < 0
}
