package cognito

import (
	"net/http"
	"time"
)

func ClearBrowserAuthorizationCookie() *http.Cookie {
	return &http.Cookie{
		Name: BrowserAuthorizationCookieName, Value: "", Path: "/", Expires: time.Unix(1, 0).UTC(), MaxAge: -1,
		Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode,
	}
}
