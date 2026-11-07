package session

import "context"

func (m *Manager) resolveForLogoutWithBrowserBinding(ctx context.Context, token string, browserHash [32]byte) (Principal, error) {
	hash, err := hashSessionToken(token)
	if err != nil {
		return Principal{}, err
	}
	if browserHash != ([32]byte{}) {
		store, ok := m.store.(BoundCognitoLogoutStore)
		if !ok && m.cognitoProtector != nil {
			return Principal{}, ErrInvalidSessionConfig
		}
		if ok {
			principal, err := store.FindBoundCognitoSessionForLogout(ctx, hash, browserHash)
			if err != ErrInvalidSession {
				return principal, err
			}
		}
		return m.useSessionWithBrowserBinding(ctx, hash, browserHash)
	} else if store, ok := m.store.(CognitoSessionStore); ok {
		principal, err := store.FindCognitoSessionForLogout(ctx, hash)
		if err != ErrInvalidSession {
			return principal, err
		}
	}
	return m.resolveSessionWithBrowserBinding(ctx, token, browserHash)
}
