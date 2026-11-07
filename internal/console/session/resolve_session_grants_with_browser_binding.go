package session

import (
	"context"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

func (m *Manager) resolveSessionGrantsWithBrowserBinding(ctx context.Context, token, clusterID string, browserHash [32]byte) (Principal, []authorization.Grant, error) {
	var unboundStore SessionGrantStore
	var boundStore BoundSessionGrantStore
	if browserHash != ([32]byte{}) {
		var ok bool
		boundStore, ok = m.store.(BoundSessionGrantStore)
		if !ok {
			return Principal{}, nil, ErrInvalidSessionConfig
		}
	} else {
		var ok bool
		unboundStore, ok = m.store.(SessionGrantStore)
		if !ok {
			return Principal{}, nil, ErrInvalidSessionConfig
		}
	}
	principal, err := m.resolveSessionWithBrowserBinding(ctx, token, browserHash)
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
	var grants []authorization.Grant
	if boundStore != nil {
		grants, err = boundStore.ResolveBoundSessionGrants(ctx, hash, browserHash, clusterID, m.now().UTC(), m.cognitoProtector)
	} else {
		grants, err = unboundStore.ResolveSessionGrants(ctx, hash, clusterID, m.now().UTC(), m.cognitoProtector)
	}
	if err != nil {
		return Principal{}, nil, err
	}
	return principal, grants, nil
}
