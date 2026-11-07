package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

var errInvalidSessionCookie = errors.New("invalid session cookie")

// findExistingSession reports whether the request already has a usable local or bound session.
func (a *SessionAPI) findExistingSession(r *http.Request, services *CognitoServices, marker [32]byte) (bool, error) {
	token, found, err := readSessionCookie(r)
	if err != nil {
		return false, errInvalidSessionCookie
	}
	if !found {
		return false, nil
	}
	if _, err := a.sessions.Resolve(r.Context(), token); err == nil {
		return true, nil
	} else if err != session.ErrInvalidSession {
		return false, err
	}
	if _, err := services.Sessions.ResolveBound(r.Context(), token, marker); err == nil {
		return true, nil
	} else if err != session.ErrInvalidSession {
		return false, err
	}
	return false, nil
}
