package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) logout(w http.ResponseWriter, r *http.Request) {
	_, token, ok := a.authorizeSession(w, r, session.AccessLogout, true)
	if !ok {
		return
	}
	if err := a.sessions.Revoke(r.Context(), token); err == session.ErrInvalidSession {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return
	} else if err != nil {
		a.writeUnavailable(w, "logout", err)
		return
	}
	clearCookies(w)
	w.WriteHeader(http.StatusNoContent)
}
