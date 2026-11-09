package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) getSession(w http.ResponseWriter, r *http.Request) {
	principal, token, binding, ok := a.authorizeSession(w, r, session.AccessSessionInfo, false)
	if !ok {
		return
	}
	administration, err := a.resolveSessionAdministration(r.Context(), principal, token, binding)
	if errors.Is(err, session.ErrInvalidSession) {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return
	}
	if err != nil {
		// The flags only decide which links the UI shows, so an outage hides them instead of failing the session.
		a.logger.Error("Console session administration unavailable", "error_type", fmt.Sprintf("%T", err))
		administration = sessionAdministration{}
	}
	a.writeJSON(w, http.StatusOK, "application/json", sessionResponse{
		AccountID: principal.AccountID, Username: principal.Username, Source: string(principal.Source), MustChangePassword: principal.MustChangePassword,
		AbsoluteExpiresAt: principal.AbsoluteExpiresAt.UTC().Format(time.RFC3339), Administration: &administration,
	})
}
