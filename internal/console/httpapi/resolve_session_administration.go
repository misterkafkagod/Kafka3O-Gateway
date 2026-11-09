package httpapi

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// resolveSessionAdministration reports which administration pages the session may open, from its current grants.
// It is a display hint for the UI; every administration endpoint still authorizes its own requests.
func (a *SessionAPI) resolveSessionAdministration(ctx context.Context, principal session.Principal, token string,
	binding [32]byte) (sessionAdministration, error) {
	if principal.MustChangePassword || a.localGrants == nil || a.activeClusters == nil {
		return sessionAdministration{}, nil
	}
	grants, err := a.resolveClusterGrants(ctx, token, binding, "")
	if err != nil {
		return sessionAdministration{}, err
	}
	global, err := authorization.ResolveRole(grants, "")
	if err != nil {
		return sessionAdministration{}, err
	}
	result := sessionAdministration{
		Access: global == authorization.RoleAdmin && principal.Source != session.SourceCognito && a.accessList != nil,
		Audit:  global == authorization.RoleAdmin && a.audit != nil,
	}
	if result.Audit || a.audit == nil {
		return result, nil
	}
	clusters, err := a.activeClusters.ListActiveClusters(ctx)
	if err != nil {
		return sessionAdministration{}, err
	}
	for _, cluster := range clusters {
		grants, err := a.resolveClusterGrants(ctx, token, binding, cluster.ID)
		if err != nil {
			return sessionAdministration{}, err
		}
		role, err := authorization.ResolveRole(grants, cluster.ID)
		if err != nil {
			return sessionAdministration{}, err
		}
		if role == authorization.RoleAdmin {
			result.Audit = true
			break
		}
	}
	return result, nil
}
