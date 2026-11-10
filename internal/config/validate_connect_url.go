package config

import (
	"errors"
	"net/url"
)

// ValidateConnectURL accepts an empty value (no Connect) or an absolute http(s) URL without credentials, query or
// fragment, so connector paths can be appended safely.
func ValidateConnectURL(raw string) error {
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("connect.url must be an http or https URL without credentials, query or fragment")
	}
	return nil
}
