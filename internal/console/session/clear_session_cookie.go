package session

import (
	"net/http"
	"time"
)

func ClearSessionCookie() *http.Cookie {
	return &http.Cookie{
		Name: CookieName, Value: "", Path: "/", Expires: time.Unix(1, 0).UTC(), MaxAge: -1,
		Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode,
	}
}
