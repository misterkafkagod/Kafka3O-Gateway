package session

import (
	"net/http"
	"time"
)

func ClearCSRFCookie() *http.Cookie {
	return &http.Cookie{
		Name: CSRFCookieName, Value: "", Path: "/", Expires: time.Unix(1, 0).UTC(), MaxAge: -1,
		Secure: true, SameSite: http.SameSiteStrictMode,
	}
}
