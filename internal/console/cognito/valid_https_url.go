package cognito

import (
	"net/url"
	"strings"
)

func validHTTPSURL(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 2048 {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.Hostname() != "" &&
		parsed.User == nil && parsed.Opaque == "" && parsed.RawQuery == "" && !parsed.ForceQuery &&
		parsed.Fragment == "" && parsed.RawFragment == ""
}
