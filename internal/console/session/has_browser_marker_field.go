package session

import (
	"net/http"
	"strings"
)

func HasBrowserMarkerField(request *http.Request) bool {
	if request == nil {
		return false
	}
	for _, header := range request.Header.Values("Cookie") {
		for _, field := range strings.Split(header, ";") {
			name, _, _ := strings.Cut(strings.TrimSpace(field), "=")
			if strings.TrimSpace(name) == BrowserMarkerCookieName {
				return true
			}
		}
	}
	return false
}
