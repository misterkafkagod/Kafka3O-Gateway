package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

var errInvalidSessionCookie = errors.New("invalid session cookie")

// findExistingSession reports whether the request already has a usable local or bound session.
func (a *SessionAPI) findExistingSession(r *http.Request) (bool, error) {
	token, found, err := readSessionCookie(r)
	if err != nil {
		return false, errInvalidSessionCookie
	}
	if !found {
		return false, nil
	}
	_, _, err = a.resolveSession(r, token, false)
	switch {
	case err == nil:
		return true, nil
	case err == session.ErrInvalidSession:
		return false, nil
	default:
		return false, err
	}
}
