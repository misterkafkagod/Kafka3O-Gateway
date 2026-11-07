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
	principal, err := m.useSessionWithBrowserBinding(ctx, hash, browserHash)
	if err == ErrInvalidSession && m.cognitoRefresh != nil {
		return m.resolveCognitoRefreshWithBrowserBinding(ctx, hash, browserHash)
	}
	return principal, err
}
