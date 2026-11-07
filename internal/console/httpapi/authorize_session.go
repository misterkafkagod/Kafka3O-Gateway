package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// authorizeSession resolves the request's session (bound Cognito or unbound local) and returns
// the verified marker digest for bound sessions so downstream actions can recheck the binding.
func (a *SessionAPI) authorizeSession(w http.ResponseWriter, r *http.Request, purpose session.AccessPurpose, mutation bool) (session.Principal, string, [32]byte, bool) {
	if mutation {
		if err := a.csrf.VerifyOrigin(r); err != nil {
			a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
			return session.Principal{}, "", [32]byte{}, false
		}
	}
	token, found, err := readSessionCookie(r)
	if err != nil || !found {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return session.Principal{}, "", [32]byte{}, false
	}
	principal, binding, err := a.resolveSession(r, token, purpose == session.AccessLogout)
	if err == errCognitoNotDiscovered {
		a.writeProblem(w, http.StatusServiceUnavailable, "sso_unavailable", "Cognito sign-in is unavailable")
		return session.Principal{}, "", [32]byte{}, false
	}
	if err == session.ErrInvalidSession {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return session.Principal{}, "", [32]byte{}, false
	}
	if err != nil {
		a.writeUnavailable(w, "resolve", err)
		return session.Principal{}, "", [32]byte{}, false
	}
	if mutation {
		if err := a.csrf.Verify(r, principal); err != nil {
			a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
			return session.Principal{}, "", [32]byte{}, false
		}
	}
	if err := session.CheckSessionAccess(principal, purpose); errors.Is(err, session.ErrPasswordChangeRequired) {
		a.writeProblem(w, http.StatusForbidden, "password_change_required", "Password change required")
		return session.Principal{}, "", [32]byte{}, false
	} else if err != nil {
		a.writeUnavailable(w, "session_access", err)
		return session.Principal{}, "", [32]byte{}, false
	}
	return principal, token, binding, true
}
