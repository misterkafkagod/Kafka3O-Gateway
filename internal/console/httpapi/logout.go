package httpapi

import (
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// logout revokes the application session first. For a Cognito session it then returns the
// configured provider logout URL; it does not claim that upstream identity providers signed out.
func (a *SessionAPI) logout(w http.ResponseWriter, r *http.Request) {
	principal, token, binding, ok := a.authorizeSession(w, r, session.AccessLogout, true)
	if !ok {
		return
	}
	services := a.cognito.Load()
	var err error
	if binding != ([32]byte{}) && services != nil {
		err = services.Sessions.RevokeBound(r.Context(), token, binding)
	} else {
		err = a.sessions.Revoke(r.Context(), token)
	}
	if err == session.ErrInvalidSession {
		clearCookies(w)
		a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
		return
	} else if err != nil {
		a.writeUnavailable(w, "logout", err)
		return
	}
	clearCookies(w)
	if principal.Source == session.SourceCognito && services != nil {
		a.writeJSON(w, http.StatusOK, "application/json", struct {
			ProviderLogoutURL string `json:"providerLogoutUrl"`
		}{ProviderLogoutURL: services.LogoutURL})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
