package main

import (
	"net/url"
	"strings"
)

func validCognitoURL(value string) (*url.URL, bool) {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 2048 {
		return nil, false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return nil, false
	}
	return parsed, true
}
