package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func (a *SessionAPI) listAdminSessions(w http.ResponseWriter, r *http.Request) {
	principal, _, ok := a.authorizeSession(w, r, session.AccessNormal, false)
	if !ok {
		return
	}
	after, limit, err := readAdminSessionListQuery(r)
	if err != nil {
		a.writeRequestError(w, err)
		return
	}
	result, err := a.listSessions.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID,
	}, after, limit)
	switch {
	case errors.Is(err, identity.ErrAdminSessionListAudit):
		a.logger.Error("Console session listing audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_list_audit_unavailable",
			"Session listing could not be audited", result.OperationID)
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
	case errors.Is(err, identity.ErrInvalidAdminSessionList):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_session_list", "Invalid session listing request", result.OperationID)
		return
	case errors.Is(err, identity.ErrInvalidAdminSessionCursor):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_session_cursor", "Invalid session listing cursor", result.OperationID)
		return
	case err != nil:
		a.logger.Error("Console session listing unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_list_unavailable", "Session listing is unavailable", result.OperationID)
		return
	}
	invalid := result.OperationID == "" || result.Sessions == nil || len(result.Sessions) > limit
	if result.NextCursor != "" && (len(result.Sessions) != limit || result.NextCursor != result.Sessions[len(result.Sessions)-1].ID) {
		invalid = true
	}
	items := make([]adminSessionResponse, 0, len(result.Sessions))
	previousID := after
	for _, item := range result.Sessions {
		normalized, nameErr := identity.NormalizeLocalUsername(item.Username)
		if identity.ValidateSessionID(item.ID) != nil || item.ID <= previousID || item.AccountID == "" ||
			identity.ValidateLocalAccountCursor(item.AccountID) != nil || nameErr != nil || normalized != item.Username ||
			(item.Source != "local" && item.Source != "cognito") ||
			item.CreatedAt.Year() < 1970 || item.AbsoluteExpiresAt.Year() > 2261 ||
			!item.CreatedAt.Before(item.IdleExpiresAt) || item.IdleExpiresAt.After(item.AbsoluteExpiresAt) ||
			item.Current != (item.ID == principal.SessionID) {
			invalid = true
		}
		previousID = item.ID
		items = append(items, adminSessionResponse{
			ID: item.ID, AccountID: item.AccountID, Username: item.Username, Source: item.Source,
			CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339Nano), IdleExpiresAt: item.IdleExpiresAt.UTC().Format(time.RFC3339Nano),
			AbsoluteExpiresAt: item.AbsoluteExpiresAt.UTC().Format(time.RFC3339Nano), IsCurrent: item.Current,
		})
	}
	if invalid {
		a.logger.Error("Console session listing returned an invalid result")
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "session_list_unavailable", "Session listing is unavailable", result.OperationID)
		return
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID string                 `json:"operationId"`
		Sessions    []adminSessionResponse `json:"sessions"`
		NextCursor  string                 `json:"nextCursor"`
	}{OperationID: result.OperationID, Sessions: items, NextCursor: result.NextCursor})
}
