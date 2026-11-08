package identity

import (
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

// validGroupMapping accepts an exact HTTPS issuer and a case-sensitive Cognito group name.
func validGroupMapping(issuer, group string) bool {
	if len(issuer) > 256 || len(group) == 0 || len(group) > 128 || !utf8.ValidString(group) ||
		strings.TrimSpace(group) != group || strings.IndexFunc(group, unicode.IsControl) >= 0 {
		return false
	}
	parsed, err := url.Parse(issuer)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil &&
		parsed.RawQuery == "" && parsed.Fragment == "" && !parsed.ForceQuery && parsed.String() == issuer &&
		strings.IndexFunc(issuer, unicode.IsSpace) < 0
}
