package identity

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func ValidateLocalAccountCursor(cursor string) error {
	if len(cursor) > 128 || !utf8.ValidString(cursor) || strings.TrimSpace(cursor) != cursor {
		return ErrInvalidAccountCursor
	}
	for _, character := range cursor {
		if unicode.IsControl(character) {
			return ErrInvalidAccountCursor
		}
	}
	return nil
}
