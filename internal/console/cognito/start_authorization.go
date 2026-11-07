package cognito

import "context"

func (m *AttemptManager) StartAuthorization(ctx context.Context) (AuthorizationStart, error) {
	return m.startAuthorizationWithBrowserBinding(ctx, [32]byte{})
}
