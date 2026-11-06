package session

import "context"

func (m *Manager) Resolve(ctx context.Context, token string) (Principal, error) {
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
	principal, err := m.store.UseSession(ctx, hash, m.now, m.idleLifetime, m.cognitoProtector)
	if err == ErrInvalidSession && m.cognitoRefresh != nil {
		return m.resolveCognitoRefresh(ctx, hash)
	}
	return principal, err
}
