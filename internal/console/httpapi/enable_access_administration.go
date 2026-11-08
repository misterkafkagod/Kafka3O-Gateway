package httpapi

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"

// EnableAccessAdministration mounts the audited grant and group-mapping routes; call it before serving.
func (a *SessionAPI) EnableAccessAdministration(list identity.ListAccessBindings, grants identity.ChangeRoleGrant,
	mappings identity.ChangeGroupMapping) error {
	if a == nil || list == nil || grants == nil || mappings == nil || a.accessList != nil {
		return ErrInvalidAPIConfig
	}
	a.accessList, a.changeGrant, a.changeMapping = list, grants, mappings
	a.mux.HandleFunc("GET /api/admin/access", a.listAccessBindings)
	a.mux.HandleFunc("PUT /api/admin/role-grants", a.changeRoleGrant)
	a.mux.HandleFunc("PUT /api/admin/group-mappings", a.changeGroupMapping)
	return nil
}
