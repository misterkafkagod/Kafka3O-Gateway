package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func readSessionCookie(r *http.Request) (token string, found bool, err error) {
	for _, cookie := range r.Cookies() {
		if cookie.Name != session.CookieName {
			continue
		}
		if found || cookie.Value == "" {
			return "", false, session.ErrInvalidSession
		}
		token, found = cookie.Value, true
	}
	return token, found, nil
}
