package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

func (m *Manager) ResolveGrants(ctx context.Context, token, clusterID string) (Principal, []authorization.Grant, error) {
	store, ok := m.store.(SessionGrantStore)
	if !ok {
		return Principal{}, nil, ErrInvalidSessionConfig
	}
	principal, err := m.Resolve(ctx, token)
	if err != nil {
		return Principal{}, nil, err
	}
	if err := CheckSessionAccess(principal, AccessNormal); err != nil {
		return Principal{}, nil, err
	}
	hash, err := hashSessionToken(token)
	if err != nil {
		return Principal{}, nil, err
	}
	grants, err := store.ResolveSessionGrants(ctx, hash, clusterID, m.now().UTC(), m.cognitoProtector)
	if err != nil {
		return Principal{}, nil, err
	}
	return principal, grants, nil
}
