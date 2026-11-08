package httpapi

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"

// EnableClusterAdministration mounts the audited cluster listing and classification routes; call it before serving.
func (a *SessionAPI) EnableClusterAdministration(list identity.ListRegisteredClusters, classify identity.ChangeClusterEnvironment) error {
	if a == nil || list == nil || classify == nil || a.clusterList != nil {
		return ErrInvalidAPIConfig
	}
	a.clusterList, a.classifyCluster = list, classify
	a.mux.HandleFunc("GET /api/admin/clusters", a.listRegisteredClusters)
	a.mux.HandleFunc("PUT /api/admin/clusters/{clusterID}/environment", a.changeClusterEnvironment)
	return nil
}
