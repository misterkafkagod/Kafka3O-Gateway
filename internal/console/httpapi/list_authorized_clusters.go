package httpapi

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// listAuthorizedClusters returns the active clusters the session may currently read, with the authoritative
// environment and effective role; Gateway addresses are never exposed to the browser.
func (a *SessionAPI) listAuthorizedClusters(w http.ResponseWriter, r *http.Request) {
	_, token, binding, ok := a.authorizeSession(w, r, session.AccessNormal, false)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.writeRequestError(w, errInvalidRequest)
		return
	}
	clusters, err := a.activeClusters.ListActiveClusters(r.Context())
	if err != nil {
		a.logger.Error("Console cluster registry unavailable", "error_type", fmt.Sprintf("%T", err))
		a.writeProblem(w, http.StatusServiceUnavailable, "clusters_unavailable", "Clusters are unavailable")
		return
	}
	authorized := make([]authorizedClusterResponse, 0, len(clusters))
	for _, cluster := range clusters {
		grants, err := a.resolveClusterGrants(r.Context(), token, binding, cluster.ID)
		if errors.Is(err, session.ErrInvalidSession) {
			clearCookies(w)
			a.writeProblem(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
			return
		}
		if err != nil {
			a.logger.Error("Console session grants unavailable", "error_type", fmt.Sprintf("%T", err))
			a.writeProblem(w, http.StatusServiceUnavailable, "clusters_unavailable", "Clusters are unavailable")
			return
		}
		role, err := authorization.ResolveRole(grants, cluster.ID)
		if err != nil {
			a.writeProblem(w, http.StatusServiceUnavailable, "clusters_unavailable", "Clusters are unavailable")
			return
		}
		if role != authorization.RoleNone {
			authorized = append(authorized, authorizedClusterResponse{cluster.ID, cluster.Name, cluster.Environment, role})
		}
	}
	a.writeJSON(w, http.StatusOK, "application/json", struct {
		Clusters []authorizedClusterResponse `json:"clusters"`
	}{authorized})
}
