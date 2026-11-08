package httpapi

import (
	"context"
	"errors"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

var errGrantsUnavailable = errors.New("session grants are unavailable")

// resolveClusterGrants resolves current grants through the same path that authenticated the session:
// the Cognito manager for marker-bound sessions, the local manager otherwise.
func (a *SessionAPI) resolveClusterGrants(ctx context.Context, token string, binding [32]byte, clusterID string) ([]authorization.Grant, error) {
	if binding != ([32]byte{}) {
		services := a.cognito.Load()
		if services == nil {
			return nil, errGrantsUnavailable
		}
		_, grants, err := services.Sessions.ResolveBoundGrants(ctx, token, binding, clusterID)
		return grants, err
	}
	_, grants, err := a.localGrants.ResolveGrants(ctx, token, clusterID)
	return grants, err
}
