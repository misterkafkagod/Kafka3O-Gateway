package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type accessGrantResponse struct {
	ClusterID string             `json:"clusterId"`
	Role      authorization.Role `json:"role"`
}

type accessIdentityResponse struct {
	ID      string                `json:"id"`
	Source  string                `json:"source"`
	Label   string                `json:"label"`
	Enabled bool                  `json:"enabled"`
	Grants  []accessGrantResponse `json:"grants"`
}

type groupGrantResponse struct {
	Issuer    string             `json:"issuer"`
	Group     string             `json:"group"`
	ClusterID string             `json:"clusterId"`
	Role      authorization.Role `json:"role"`
}

func (a *SessionAPI) listAccessBindings(w http.ResponseWriter, r *http.Request) {
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
	result, err := a.accessList.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID, BrowserBindingHash: binding,
	}, after, limit)
	switch {
	case errors.Is(err, identity.ErrAccessListAudit):
		a.logger.Error("Console access listing audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "access_list_audit_unavailable", "Access listing could not be audited", result.OperationID)
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
	case errors.Is(err, identity.ErrInvalidAccessList):
		a.writeAccountProblem(w, http.StatusBadRequest, "invalid_access_list", "Invalid access listing request", result.OperationID)
		return
	case err != nil:
		a.logger.Error("Console access listing unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "access_list_unavailable", "Access listing is unavailable", result.OperationID)
		return
	}
	if result.OperationID == "" || result.Identities == nil || len(result.Identities) > limit || result.GroupGrants == nil ||
		(result.NextCursor != "" && result.NextCursor != result.Identities[len(result.Identities)-1].ID) {
		a.logger.Error("Console access listing returned an invalid result")
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "access_list_unavailable", "Access listing is unavailable", result.OperationID)
		return
	}
	identities := make([]accessIdentityResponse, 0, len(result.Identities))
	for _, entry := range result.Identities {
		grants := make([]accessGrantResponse, 0, len(entry.Grants))
		for _, grant := range entry.Grants {
			grants = append(grants, accessGrantResponse{ClusterID: grant.ClusterID, Role: grant.Role})
		}
		identities = append(identities, accessIdentityResponse{ID: entry.ID, Source: entry.Source, Label: entry.Label, Enabled: entry.Enabled, Grants: grants})
	}
	groups := make([]groupGrantResponse, 0, len(result.GroupGrants))
	for _, mapping := range result.GroupGrants {
		groups = append(groups, groupGrantResponse{Issuer: mapping.Issuer, Group: mapping.Group, ClusterID: mapping.Grant.ClusterID, Role: mapping.Grant.Role})
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID          string                   `json:"operationId"`
		PolicyRevision       int64                    `json:"policyRevision"`
		Identities           []accessIdentityResponse `json:"identities"`
		NextCursor           string                   `json:"nextCursor"`
		GroupGrants          []groupGrantResponse     `json:"groupGrants"`
		GroupGrantsTruncated bool                     `json:"groupGrantsTruncated"`
	}{result.OperationID, result.PolicyRevision, identities, result.NextCursor, groups, result.GroupGrantsTruncated})
}
