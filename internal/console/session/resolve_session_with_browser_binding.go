package session

import "context"

func (m *Manager) resolveSessionWithBrowserBinding(ctx context.Context, token string, browserHash [32]byte) (Principal, error) {
	if ctx == nil {
		return Principal{}, ErrInvalidSessionConfig
	}
	hash, err := hashSessionToken(token)
	if err != nil {
		return Principal{}, err
	}
	now := m.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Add(m.idleLifetime).Year() > 2261 {
		return Principal{}, ErrInvalidSessionConfig
	}
	var principal Principal
	if browserHash != ([32]byte{}) {
		store, ok := m.store.(BoundSessionStore)
		if !ok {
			return Principal{}, ErrInvalidSessionConfig
		}
		principal, err = store.UseBoundSession(ctx, hash, browserHash, m.now, m.idleLifetime, m.cognitoProtector)
	} else {
		principal, err = m.store.UseSession(ctx, hash, m.now, m.idleLifetime, m.cognitoProtector)
	}
	if err == ErrInvalidSession && m.cognitoRefresh != nil {
		return m.resolveCognitoRefreshWithBrowserBinding(ctx, hash, browserHash)
	}
	return principal, err
}
