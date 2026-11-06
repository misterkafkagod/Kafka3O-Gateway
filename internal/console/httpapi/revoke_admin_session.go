package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) revokeAdminSession(w http.ResponseWriter, r *http.Request) {
	principal, _, ok := a.authorizeSession(w, r, session.AccessNormal, true)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			a.writeRequestError(w, errInvalidRequest)
			return
		}
	}
	target := r.PathValue("sessionID")
	self := target == principal.SessionID
	result, err := a.revokeAdmin.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID,
	}, target)
	switch {
	case errors.Is(err, identity.ErrAdminSessionRevokeUnknown):
		if self {
			clearCookies(w)
		}
		a.logger.Error("Console session revocation outcome unknown", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_revoke_outcome_unknown",
			"Session revocation outcome is unknown; investigate before retrying", result.OperationID)
		return
	case errors.Is(err, identity.ErrAdminSessionRevokeAudit):
		a.logger.Error("Console session revocation audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_revoke_audit_unavailable",
			"Session revocation could not be audited; investigate before retrying", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidAdminSession):
		clearCookies(w)
		a.writeAccountProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required", result.OperationID)
		return
	case errors.Is(err, identity.ErrGlobalAdminRequired):
		a.writeAccountProblem(w, http.StatusForbidden, "global_admin_required", "Global administrator access required", result.OperationID)
		return
	case errors.Is(err, identity.ErrAdminPasswordChange):
		a.writeAccountProblem(w, http.StatusForbidden, "password_change_required", "Password change required", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidSessionID):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_session", "Invalid session identifier", result.OperationID)
		return
	case errors.Is(err, identity.ErrAdminSessionUnavailable):
		a.writeAccountProblem(w, http.StatusNotFound, "session_unavailable", "Session is unavailable or expired", result.OperationID)
		return
	case err != nil:
		if self {
			clearCookies(w)
		}
		a.logger.Error("Console session revocation unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_revoke_outcome_unknown",
			"Session revocation may have an unknown outcome; investigate before retrying", result.OperationID)
		return
	}
	if result.OperationID == "" {
		if self {
			clearCookies(w)
		}
		a.logger.Error("Console session revocation returned no operation ID")
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_revoke_outcome_unknown",
			"Session revocation may have an unknown outcome; investigate before retrying", "")
		return
	}
	if self {
		clearCookies(w)
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID    string `json:"operationId"`
		SignInRequired bool   `json:"signInRequired"`
	}{OperationID: result.OperationID, SignInRequired: self})
}
