package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) listLocalAccounts(w http.ResponseWriter, r *http.Request) {
	principal, _, binding, ok := a.authorizeSession(w, r, session.AccessNormal, false)
	if !ok {
		return
	}
	if !a.requireLocalAdministrator(w, principal) {
		return
	}
	after, limit, err := readLocalAccountListQuery(r)
	if err != nil {
		a.writeRequestError(w, err)
		return
	}
	result, err := a.list.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID, BrowserBindingHash: binding,
	}, after, limit)
	switch {
	case errors.Is(err, identity.ErrAccountListAudit):
		a.logger.Error("Console account listing audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_list_audit_unavailable",
			"Account listing could not be audited", result.OperationID)
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
	case errors.Is(err, identity.ErrInvalidAccountList):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_account_list", "Invalid account listing request", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidAccountCursor):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_account_cursor", "Invalid account listing cursor", result.OperationID)
		return
	case err != nil:
		a.logger.Error("Console account listing unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_list_unavailable", "Account listing is unavailable", result.OperationID)
		return
	}
	invalid := result.OperationID == "" || result.Accounts == nil || len(result.Accounts) > limit
	if result.NextCursor != "" && (len(result.Accounts) != limit || result.NextCursor != result.Accounts[len(result.Accounts)-1].ID) {
		invalid = true
	}
	accounts := make([]localAccountResponse, 0, len(result.Accounts))
	seenIDs := make(map[string]struct{}, len(result.Accounts))
	var previousUsername string
	for _, account := range result.Accounts {
		normalized, nameErr := identity.NormalizeLocalUsername(account.Username)
		_, duplicateID := seenIDs[account.ID]
		if account.ID == "" || identity.ValidateLocalAccountCursor(account.ID) != nil || nameErr != nil || normalized != account.Username ||
			duplicateID || (previousUsername != "" && previousUsername >= account.Username) {
			invalid = true
		}
		seenIDs[account.ID] = struct{}{}
		previousUsername = account.Username
		accounts = append(accounts, localAccountResponse{
			ID: account.ID, Username: account.Username, Enabled: account.Enabled, MustChangePassword: account.MustChangePassword,
		})
	}
	if invalid {
		a.logger.Error("Console account listing returned an invalid result")
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "account_list_unavailable", "Account listing is unavailable", result.OperationID)
		return
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID string                 `json:"operationId"`
		Accounts    []localAccountResponse `json:"accounts"`
		NextCursor  string                 `json:"nextCursor"`
	}{OperationID: result.OperationID, Accounts: accounts, NextCursor: result.NextCursor})
}
