package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) createLocalAccount(w http.ResponseWriter, r *http.Request) {
	principal, _, ok := a.authorizeSession(w, r, session.AccessNormal, true)
	if !ok {
		return
	}
	var body createLocalAccountBody
	if err := readJSON(w, r, &body); err != nil {
		a.writeRequestError(w, err)
		return
	}
	result, err := a.create.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID,
	}, body.Username, body.TemporaryPassword)
	switch {
	case errors.Is(err, identity.ErrAccountCreationAudit):
		a.logger.Error("Console account creation audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_creation_audit_unavailable",
			"Account creation could not be audited; investigate before retrying", result.OperationID)
		return
	case errors.Is(err, identity.ErrAccountCreationUnknown):
		a.logger.Error("Console account creation outcome unknown", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_creation_outcome_unknown",
			"Account creation outcome is unknown; investigate before retrying", result.OperationID)
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
	case errors.Is(err, identity.ErrInvalidAccountCreation), errors.Is(err, identity.ErrAccountCreationExpired):
		a.writeAccountProblem(w, http.StatusConflict, "account_creation_expired", "Account creation expired or was already used", result.OperationID)
		return
	case errors.Is(err, identity.ErrUsernameTaken):
		a.writeAccountProblem(w, http.StatusConflict, "username_in_use", "Local username is already in use", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidUsername):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_username", "Choose a valid local username", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidPassword):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_password", "Choose a password meeting the password policy", result.OperationID)
		return
	case err != nil:
		a.logger.Error("Console account creation unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_creation_outcome_unknown",
			"Account creation may have an unknown outcome; investigate before retrying", result.OperationID)
		return
	}
	account := result.Account
	normalized, nameErr := identity.NormalizeLocalUsername(account.Username)
	if result.OperationID == "" || account.ID == "" || nameErr != nil || normalized != account.Username ||
		!account.Enabled || !account.MustChangePassword || account.CredentialVersion != 1 || account.PasswordHash != "" {
		a.logger.Error("Console account creation returned an invalid result")
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_creation_outcome_unknown",
			"Account creation may have an unknown outcome; investigate before retrying", result.OperationID)
		return
	}
	a.writeJSON(w, http.StatusCreated, "application/json", struct {
		OperationID string              `json:"operationId"`
		Account     createdLocalAccount `json:"account"`
	}{
		OperationID: result.OperationID,
		Account:     createdLocalAccount{ID: account.ID, Username: account.Username, Enabled: account.Enabled, MustChangePassword: account.MustChangePassword},
	})
}
