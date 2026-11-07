package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

func (m *Manager) ResolveGrants(ctx context.Context, token, clusterID string) (Principal, []authorization.Grant, error) {
	return m.resolveSessionGrantsWithBrowserBinding(ctx, token, clusterID, [32]byte{})
}
