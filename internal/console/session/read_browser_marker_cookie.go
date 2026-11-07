package session

import (
	"net/http"
	"strings"
)

func ReadBrowserMarkerCookie(request *http.Request) ([32]byte, error) {
	if request == nil {
		return [32]byte{}, ErrInvalidBrowserMarker
	}
	var hash [32]byte
	var found bool
	for _, header := range request.Header.Values("Cookie") {
		for _, field := range strings.Split(header, ";") {
			name, value, hasValue := strings.Cut(strings.TrimSpace(field), "=")
			if strings.TrimSpace(name) != BrowserMarkerCookieName {
				continue
			}
			if found || !hasValue || name != BrowserMarkerCookieName {
				return [32]byte{}, ErrInvalidBrowserMarker
			}
			found = true
			var err error
			hash, err = HashBrowserMarker(value)
			if err != nil {
				return [32]byte{}, err
			}
		}
	}
	if !found {
		return [32]byte{}, ErrInvalidBrowserMarker
	}
	return hash, nil
}
