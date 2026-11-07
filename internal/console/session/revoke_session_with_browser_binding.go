package session

import "context"

func (m *Manager) revokeSessionWithBrowserBinding(ctx context.Context, token string, browserHash [32]byte) error {
	var boundStore BoundSessionRevokeStore
	if browserHash != ([32]byte{}) {
		var ok bool
		boundStore, ok = m.store.(BoundSessionRevokeStore)
		if !ok {
			return ErrInvalidSessionConfig
		}
	}
	hash, err := hashSessionToken(token)
	if err != nil {
		return err
	}
	eventID, _, err := newSessionValue(m.random, 16)
	if err != nil {
		return err
	}
	if boundStore != nil {
		return boundStore.RevokeBoundSession(ctx, hash, browserHash, eventID, m.now().UTC())
	}
	return m.store.RevokeSession(ctx, hash, eventID, m.now().UTC())
}
