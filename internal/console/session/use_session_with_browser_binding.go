package session

import "context"

func (m *Manager) useSessionWithBrowserBinding(ctx context.Context, tokenHash, browserHash [32]byte) (Principal, error) {
	if ctx == nil {
		return Principal{}, ErrInvalidSessionConfig
	}
	now := m.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Add(m.idleLifetime).Year() > 2261 {
		return Principal{}, ErrInvalidSessionConfig
	}
	if browserHash != ([32]byte{}) {
		store, ok := m.store.(BoundSessionStore)
		if !ok {
			return Principal{}, ErrInvalidSessionConfig
		}
		return store.UseBoundSession(ctx, tokenHash, browserHash, m.now, m.idleLifetime, m.cognitoProtector)
	}
	return m.store.UseSession(ctx, tokenHash, m.now, m.idleLifetime, m.cognitoProtector)
}
