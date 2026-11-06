package session

import (
	"net/http"
	"time"
)

const CookieName = "__Host-kafka3o_session"

func SessionCookie(token string, absoluteExpiry time.Time) (*http.Cookie, error) {
	if _, err := hashSessionToken(token); err != nil {
		return nil, err
	}
	if absoluteExpiry.IsZero() || absoluteExpiry.Year() < 1970 || absoluteExpiry.Year() > 2261 {
		return nil, ErrInvalidSessionConfig
	}
	return &http.Cookie{
		Name: CookieName, Value: token, Path: "/", Expires: absoluteExpiry.UTC(),
		Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode,
	}, nil
}
