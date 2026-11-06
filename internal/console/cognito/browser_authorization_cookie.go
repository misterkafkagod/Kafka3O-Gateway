package cognito

import (
	"net/http"
	"time"
)

const BrowserAuthorizationCookieName = "__Host-kafka3o_sso"

func BrowserAuthorizationCookie(start AuthorizationStart) (*http.Cookie, error) {
	if start.proof == nil {
		return nil, ErrInvalidAttempt
	}
	now, err := start.proof.manager.validateBrowserAuthorization(start)
	if err != nil {
		return nil, err
	}
	remaining := start.ExpiresAt.Sub(now)
	return &http.Cookie{
		Name: BrowserAuthorizationCookieName, Value: start.BrowserBinding, Path: "/",
		Expires: start.ExpiresAt.UTC(), MaxAge: int((remaining + time.Second - 1) / time.Second),
		Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode,
	}, nil
}
