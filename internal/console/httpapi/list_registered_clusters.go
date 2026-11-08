package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type registeredClusterResponse struct {
	ID          string                    `json:"id"`
	Name        string                    `json:"name"`
	GatewayURL  string                    `json:"gatewayUrl"`
	Environment authorization.Environment `json:"environment"`
	Active      bool                      `json:"active"`
}

func (a *SessionAPI) listRegisteredClusters(w http.ResponseWriter, r *http.Request) {
	principal, _, binding, ok := a.authorizeSession(w, r, session.AccessNormal, false)
	if !ok {
		return
	}
	if !a.requireLocalAdministrator(w, principal) {
		return
	}
	if r.URL.RawQuery != "" {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	result, err := a.clusterList.Execute(r.Context(), identity.LocalAdminActor{
		AccountID: principal.AccountID, SessionID: principal.SessionID, BrowserBindingHash: binding,
	})
	switch {
	case errors.Is(err, identity.ErrClusterListAudit):
		a.logger.Error("Console cluster listing audit unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "cluster_list_audit_unavailable", "Cluster listing could not be audited", result.OperationID)
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
	case err != nil || result.OperationID == "" || result.Clusters == nil:
		a.logger.Error("Console cluster listing unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeAccountProblem(w, http.StatusServiceUnavailable, "cluster_list_unavailable", "Cluster listing is unavailable", result.OperationID)
		return
	}
	clusters := make([]registeredClusterResponse, 0, len(result.Clusters))
	for _, cluster := range result.Clusters {
		clusters = append(clusters, registeredClusterResponse{cluster.ID, cluster.Name, cluster.GatewayURL, cluster.Environment, cluster.Active})
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		OperationID    string                      `json:"operationId"`
		PolicyRevision int64                       `json:"policyRevision"`
		Clusters       []registeredClusterResponse `json:"clusters"`
	}{result.OperationID, result.PolicyRevision, clusters})
}
