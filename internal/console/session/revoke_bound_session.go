package session

import "context"

func (m *Manager) RevokeBound(ctx context.Context, token string, browserHash [32]byte) error {
	if browserHash == ([32]byte{}) {
		return ErrInvalidSession
	}
	return m.revokeSessionWithBrowserBinding(ctx, token, browserHash)
}
