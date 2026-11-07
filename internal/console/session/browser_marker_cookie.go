package session

import "net/http"

const browserMarkerMaxAge = 400 * 24 * 60 * 60

func BrowserMarkerCookie(value string) (*http.Cookie, error) {
	if _, err := HashBrowserMarker(value); err != nil {
		return nil, err
	}
	return &http.Cookie{
		Name: BrowserMarkerCookieName, Value: value, Path: "/", MaxAge: browserMarkerMaxAge,
		Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode,
	}, nil
}
