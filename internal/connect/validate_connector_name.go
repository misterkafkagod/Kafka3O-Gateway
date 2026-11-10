package connect

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// ValidateConnectorName rejects names Connect cannot address: "/" makes Jetty refuse the path, and control
// characters or surrounding spaces never come from Connect's own listing.
func ValidateConnectorName(name string) error {
	if name == "" || utf8.RuneCountInString(name) > 249 || !utf8.ValidString(name) || strings.TrimSpace(name) != name ||
		strings.ContainsRune(name, '/') || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return ErrInvalidConnectorName
	}
	return nil
}
