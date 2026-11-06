package identity

import (
	"strings"
	"unicode/utf8"
)

func ValidateLocalPassword(password string) error {
	if !utf8.ValidString(password) || len(password) > passwordMaxBytes ||
		utf8.RuneCountInString(password) < passwordMinRunes || strings.TrimSpace(password) == "" {
		return ErrInvalidPassword
	}
	return nil
}
