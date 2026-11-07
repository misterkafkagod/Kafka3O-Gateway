package session

import "context"

func (m *Manager) Revoke(ctx context.Context, token string) error {
	return m.revokeSessionWithBrowserBinding(ctx, token, [32]byte{})
}
