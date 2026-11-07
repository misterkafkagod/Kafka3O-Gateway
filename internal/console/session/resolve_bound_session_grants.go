package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

func (m *Manager) ResolveBoundGrants(ctx context.Context, token string, browserHash [32]byte, clusterID string) (Principal, []authorization.Grant, error) {
	if browserHash == ([32]byte{}) {
		return Principal{}, nil, ErrInvalidSession
	}
	return m.resolveSessionGrantsWithBrowserBinding(ctx, token, clusterID, browserHash)
}
