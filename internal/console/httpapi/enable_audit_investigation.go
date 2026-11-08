package httpapi

// EnableAuditInvestigation mounts audit search and export; it requires EnableGatewayForwarding for grants.
func (a *SessionAPI) EnableAuditInvestigation(audit AuditStore) error {
	if a == nil || audit == nil || a.localGrants == nil || a.audit != nil {
		return ErrInvalidAPIConfig
	}
	a.audit = audit
	a.mux.HandleFunc("GET /api/audit", a.listAuditEvents)
	a.mux.HandleFunc("GET /api/audit/export", a.exportAuditEvents)
	return nil
}
