package session

import (
	"net/http"
	"time"
)

const CSRFCookieName = "__Host-kafka3o_csrf"

func CSRFCookie(token string, absoluteExpiry time.Time) (*http.Cookie, error) {
	if _, err := hashSessionToken(token); err != nil {
		return nil, err
	}
	if absoluteExpiry.IsZero() || absoluteExpiry.Year() < 1970 || absoluteExpiry.Year() > 2261 {
		return nil, ErrInvalidSessionConfig
	}
	return &http.Cookie{
		Name: CSRFCookieName, Value: token, Path: "/", Expires: absoluteExpiry.UTC(),
		Secure: true, SameSite: http.SameSiteStrictMode,
	}, nil
}
