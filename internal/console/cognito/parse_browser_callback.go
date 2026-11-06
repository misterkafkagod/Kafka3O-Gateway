package cognito

import (
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

func ParseBrowserCallback(rawQuery string) (BrowserCallback, error) {
	if len(rawQuery) == 0 || len(rawQuery) > 8192 {
		return BrowserCallback{}, ErrInvalidBrowserCallback
	}
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return BrowserCallback{}, ErrInvalidBrowserCallback
	}
	for key, values := range query {
		switch key {
		case "state", "code", "error", "error_description", "error_uri":
		default:
			return BrowserCallback{}, ErrInvalidBrowserCallback
		}
		if len(values) != 1 {
			return BrowserCallback{}, ErrInvalidBrowserCallback
		}
	}
	state := query.Get("state")
	if _, err := hashAttemptValue(state); err != nil {
		return BrowserCallback{}, ErrInvalidBrowserCallback
	}
	code, hasCode := query["code"]
	providerError, hasError := query["error"]
	_, hasDescription := query["error_description"]
	_, hasErrorURI := query["error_uri"]
	if hasCode == hasError {
		return BrowserCallback{}, ErrInvalidBrowserCallback
	}
	if hasCode {
		if hasDescription || hasErrorURI || !validAuthorizationCode(code[0]) {
			return BrowserCallback{}, ErrInvalidBrowserCallback
		}
		return BrowserCallback{State: state, Code: code[0]}, nil
	}
	for _, field := range []struct {
		value string
		limit int
	}{
		{providerError[0], 128},
		{query.Get("error_description"), 2048},
		{query.Get("error_uri"), 2048},
	} {
		if len(field.value) > field.limit || !utf8.ValidString(field.value) ||
			strings.IndexFunc(field.value, unicode.IsControl) >= 0 {
			return BrowserCallback{}, ErrInvalidBrowserCallback
		}
	}
	if providerError[0] == "" {
		return BrowserCallback{}, ErrInvalidBrowserCallback
	}
	return BrowserCallback{State: state, Denied: true}, nil
}
