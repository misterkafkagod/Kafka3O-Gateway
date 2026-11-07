package session

import "context"

func (m *Manager) Resolve(ctx context.Context, token string) (Principal, error) {
	return m.resolveSessionWithBrowserBinding(ctx, token, [32]byte{})
}
