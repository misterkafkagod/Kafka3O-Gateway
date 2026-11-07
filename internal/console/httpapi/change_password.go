package httpapi

import (
	"errors"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) changePassword(w http.ResponseWriter, r *http.Request) {
	principal, _, _, ok := a.authorizeSession(w, r, session.AccessPasswordChange, true)
	if !ok {
		return
	}
	var body changePasswordBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	client, err := clientAddress(r)
	if err != nil {
		a.writeUnavailable(w, "client_address", err)
		return
	}
	err = a.change.Execute(r.Context(), principal.AccountID, principal.Username, body.CurrentPassword, body.NewPassword, client)
	switch {
	case errors.Is(err, identity.ErrAuthenticationFailed):
		a.writeProblem(w, http.StatusUnauthorized, "authentication_failed", "Authentication failed")
		return
	case errors.Is(err, identity.ErrAuthenticationThrottled):
		w.Header().Set("Retry-After", "900")
		a.writeProblem(w, http.StatusTooManyRequests, "authentication_throttled", "Authentication temporarily unavailable")
		return
	case errors.Is(err, identity.ErrInvalidPassword), errors.Is(err, identity.ErrPasswordUnchanged):
		a.writeProblem(w, http.StatusBadRequest, "invalid_password", "Choose a different password meeting the password policy")
		return
	case err != nil:
		a.writeUnavailable(w, "change_password", err)
		return
	}
	clearCookies(w)
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		SignInRequired bool `json:"signInRequired"`
	}{SignInRequired: true})
}
