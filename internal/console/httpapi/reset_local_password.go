package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) resetLocalPassword(w http.ResponseWriter, r *http.Request) {
	principal, _, ok := a.authorizeSession(w, r, session.AccessNormal, true)
	if !ok {
		return
	}
	var body resetPasswordBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	targetID := r.PathValue("accountID")
	result, err := a.reset.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID,
	}, targetID, body.TemporaryPassword)
	switch {
	case errors.Is(err, identity.ErrPasswordResetAudit):
		a.logger.Error("Console password reset audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writePasswordResetProblem(w, http.StatusServiceUnavailable, "password_reset_audit_unavailable",
			"Password reset could not be audited; investigate before retrying", result.OperationID)
		return
	case errors.Is(err, identity.ErrPasswordResetUnknown):
		a.logger.Error("Console password reset outcome unknown", "error_type", fmt.Sprintf("%T", err))
		a.writePasswordResetProblem(w, http.StatusServiceUnavailable, "password_reset_outcome_unknown",
			"Password reset outcome is unknown; investigate before retrying", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidAdminSession):
		clearCookies(w)
		a.writePasswordResetProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required", result.OperationID)
		return
	case errors.Is(err, identity.ErrGlobalAdminRequired):
		a.writePasswordResetProblem(w, http.StatusForbidden, "global_admin_required", "Global administrator access required", result.OperationID)
		return
	case errors.Is(err, identity.ErrAdminPasswordChange):
		a.writePasswordResetProblem(w, http.StatusForbidden, "password_change_required", "Password change required", result.OperationID)
		return
	case errors.Is(err, identity.ErrPasswordResetThrottled):
		w.Header().Set("Retry-After", "900")
		a.writePasswordResetProblem(w, http.StatusTooManyRequests, "password_reset_throttled", "Password reset temporarily unavailable", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidPassword):
		a.writePasswordResetProblem(w, http.StatusBadRequest, "invalid_password", "Choose a password meeting the password policy", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidLocalAccount):
		a.writePasswordResetProblem(w, http.StatusBadRequest, "invalid_account", "Invalid local account", result.OperationID)
		return
	case errors.Is(err, identity.ErrAccountNotFound):
		a.writePasswordResetProblem(w, http.StatusNotFound, "account_not_found", "Local account not found", result.OperationID)
		return
	case errors.Is(err, identity.ErrLocalAccountChanged):
		a.writePasswordResetProblem(w, http.StatusConflict, "account_changed", "Local account changed; inspect its current state", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidPasswordReset), errors.Is(err, identity.ErrPasswordResetExpired):
		a.writePasswordResetProblem(w, http.StatusConflict, "password_reset_expired", "Password reset expired or was already used", result.OperationID)
		return
	case err != nil:
		a.logger.Error("Console password reset unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writePasswordResetProblem(w, http.StatusServiceUnavailable, "password_reset_outcome_unknown",
			"Password reset may have an unknown outcome; investigate before retrying", result.OperationID)
		return
	}
	if result.OperationID == "" {
		a.logger.Error("Console password reset returned no operation ID")
		a.writePasswordResetProblem(w, http.StatusServiceUnavailable, "password_reset_outcome_unknown",
			"Password reset may have an unknown outcome; investigate before retrying", "")
		return
	}
	signInRequired := targetID == principal.AccountID
	if signInRequired {
		clearCookies(w)
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID    string `json:"operationId"`
		SignInRequired bool   `json:"signInRequired"`
	}{OperationID: result.OperationID, SignInRequired: signInRequired})
}
