package httpapi

import (
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) getSession(w http.ResponseWriter, r *http.Request) {
	principal, _, ok := a.authorizeSession(w, r, session.AccessSessionInfo, false)
	if !ok {
		return
	}
	a.writeJSON(w, http.StatusOK, "application/json", sessionResponse{
		AccountID: principal.AccountID, Username: principal.Username, Source: string(principal.Source), MustChangePassword: principal.MustChangePassword,
		AbsoluteExpiresAt: principal.AbsoluteExpiresAt.UTC().Format(time.RFC3339),
	})
}
