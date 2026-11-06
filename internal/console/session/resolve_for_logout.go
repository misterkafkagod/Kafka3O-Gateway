package session

import (
	"context"
	"errors"
)

func (m *Manager) ResolveForLogout(ctx context.Context, token string) (Principal, error) {
	hash, err := hashSessionToken(token)
	if err != nil {
		return Principal{}, err
	}
	if store, ok := m.store.(CognitoSessionStore); ok {
		principal, err := store.FindCognitoSessionForLogout(ctx, hash)
		if !errors.Is(err, ErrInvalidSession) {
			return principal, err
		}
	}
	return m.Resolve(ctx, token)
}
