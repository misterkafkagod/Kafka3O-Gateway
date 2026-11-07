package session

import "context"

func (m *Manager) ResolveBoundForLogout(ctx context.Context, token string, browserHash [32]byte) (Principal, error) {
	if browserHash == ([32]byte{}) {
		return Principal{}, ErrInvalidSession
	}
	return m.resolveForLogoutWithBrowserBinding(ctx, token, browserHash)
}
