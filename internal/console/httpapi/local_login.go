package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) localLogin(w http.ResponseWriter, r *http.Request) {
	if err := a.csrf.VerifyOrigin(r); err != nil {
		a.writeProblem(w, http.StatusForbidden, "csrf_rejected", "Request verification failed")
		return
	}
	existing, found, err := readSessionCookie(r)
	if err != nil {
		clearCookies(w)
		a.writeProblem(w, http.StatusBadRequest, "invalid_session_cookie", "Invalid session cookie")
		return
	}
	if found {
		_, err := a.sessions.Resolve(r.Context(), existing)
		if err == nil {
			a.writeProblem(w, http.StatusConflict, "already_signed_in", "Sign out before signing in again")
			return
		}
		if !errors.Is(err, session.ErrInvalidSession) {
			a.writeUnavailable(w, "resolve_existing", err)
			return
		}
		clearCookies(w)
	}
	var body localLoginBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	client, err := clientAddress(r)
	if err != nil {
		a.writeUnavailable(w, "client_address", err)
		return
	}
	credentials, err := a.login.Execute(r.Context(), body.Username, body.Password, client)
	switch {
	case errors.Is(err, identity.ErrAuthenticationFailed), errors.Is(err, session.ErrInvalidSession):
		a.writeProblem(w, http.StatusUnauthorized, "authentication_failed", "Authentication failed")
		return
	case errors.Is(err, identity.ErrAuthenticationThrottled):
		w.Header().Set("Retry-After", "900")
		a.writeProblem(w, http.StatusTooManyRequests, "authentication_throttled", "Authentication temporarily unavailable")
		return
	case err != nil:
		a.writeUnavailable(w, "login", err)
		return
	}
	sessionCookie, err := session.SessionCookie(credentials.Token, credentials.Principal.AbsoluteExpiresAt)
	if err != nil {
		a.writeUnavailable(w, "session_cookie", err)
		return
	}
	csrfCookie, err := session.CSRFCookie(credentials.CSRFToken, credentials.Principal.AbsoluteExpiresAt)
	if err != nil {
		a.writeUnavailable(w, "csrf_cookie", err)
		return
	}
	http.SetCookie(w, sessionCookie)
	http.SetCookie(w, csrfCookie)
	a.writeJSON(w, http.StatusOK, "application/json", sessionResponse{
		AccountID: credentials.Principal.AccountID, Username: credentials.Principal.Username, Source: string(session.SourceLocal),
		MustChangePassword: credentials.Principal.MustChangePassword,
		AbsoluteExpiresAt:  credentials.Principal.AbsoluteExpiresAt.UTC().Format(time.RFC3339),
	})
}
