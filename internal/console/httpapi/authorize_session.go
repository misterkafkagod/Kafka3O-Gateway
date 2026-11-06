package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) authorizeSession(w http.ResponseWriter, r *http.Request, purpose session.AccessPurpose, mutation bool) (session.Principal, string, bool) {
	if mutation {
		if err := a.csrf.VerifyOrigin(r); err != nil {
			a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
			return session.Principal{}, "", false
		}
	}
	token, found, err := readSessionCookie(r)
	if err != nil || !found {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return session.Principal{}, "", false
	}
	principal, err := a.sessions.Resolve(r.Context(), token)
	if errors.Is(err, session.ErrInvalidSession) {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return session.Principal{}, "", false
	}
	if err != nil {
		a.writeUnavailable(w, "resolve", err)
		return session.Principal{}, "", false
	}
	if mutation {
		if err := a.csrf.Verify(r, principal); err != nil {
			a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
			return session.Principal{}, "", false
		}
	}
	if err := session.CheckSessionAccess(principal, purpose); errors.Is(err, session.ErrPasswordChangeRequired) {
		a.writeProblem(w, http.StatusForbidden, "password_change_required", "Password change required")
		return session.Principal{}, "", false
	} else if err != nil {
		a.writeUnavailable(w, "session_access", err)
		return session.Principal{}, "", false
	}
	return principal, token, true
}
