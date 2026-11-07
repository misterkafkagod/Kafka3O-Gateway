package session

import "context"

func (m *Manager) ResolveForLogout(ctx context.Context, token string) (Principal, error) {
	return m.resolveForLogoutWithBrowserBinding(ctx, token, [32]byte{})
}
