package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) setLocalAccountEnabled(w http.ResponseWriter, r *http.Request) {
	principal, _, binding, ok := a.authorizeSession(w, r, session.AccessNormal, true)
	if !ok {
		return
	}
	if !a.requireLocalAdministrator(w, principal) {
		return
	}
	var body setLocalAccountEnabledBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	if !body.Enabled.Present {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	targetID := r.PathValue("accountID")
	enabled := body.Enabled.Value
	selfDisable := !enabled && targetID == principal.AccountID
	result, err := a.setEnabled.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID, BrowserBindingHash: binding,
	}, targetID, enabled)
	switch {
	case errors.Is(err, identity.ErrAccountStateUnknown):
		if selfDisable {
			clearCookies(w)
		}
		a.logger.Error("Console account state outcome unknown", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_state_outcome_unknown",
			"Account state outcome is unknown; investigate before retrying", result.OperationID)
		return
	case errors.Is(err, identity.ErrAccountStateAudit):
		a.logger.Error("Console account state audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_state_audit_unavailable",
			"Account state change could not be audited; investigate before retrying", result.OperationID)
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
	case errors.Is(err, identity.ErrLastLocalGlobalAdmin):
		a.writeAccountProblem(w, http.StatusConflict, "last_local_global_admin",
			"The last enabled local global administrator cannot be disabled", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidLocalAccount):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_account", "Invalid local account", result.OperationID)
		return
	case errors.Is(err, identity.ErrAccountNotFound):
		a.writeAccountProblem(w, http.StatusNotFound, "account_not_found", "Local account not found", result.OperationID)
		return
	case err != nil:
		if selfDisable {
			clearCookies(w)
		}
		a.logger.Error("Console account state change unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_state_outcome_unknown",
			"Account state may have an unknown outcome; investigate before retrying", result.OperationID)
		return
	}
	if result.OperationID == "" {
		if selfDisable {
			clearCookies(w)
		}
		a.logger.Error("Console account state change returned no operation ID")
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_state_outcome_unknown",
			"Account state may have an unknown outcome; investigate before retrying", "")
		return
	}
	if selfDisable {
		clearCookies(w)
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID    string `json:"operationId"`
		Enabled        bool   `json:"enabled"`
		SignInRequired bool   `json:"signInRequired"`
	}{OperationID: result.OperationID, Enabled: enabled, SignInRequired: selfDisable})
}
