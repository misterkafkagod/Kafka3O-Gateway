package httpapi

// EnableGatewayForwarding mounts the authorized cluster list for signed-in sessions; call it before serving.
func (a *SessionAPI) EnableGatewayForwarding(clusters ActiveClusters, grants LocalSessionGrants) error {
	if a == nil || clusters == nil || grants == nil || a.activeClusters != nil {
		return ErrInvalidAPIConfig
	}
	a.activeClusters, a.localGrants = clusters, grants
	a.mux.HandleFunc("GET /api/clusters", a.listAuthorizedClusters)
	return nil
}
