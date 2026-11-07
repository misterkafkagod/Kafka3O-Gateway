package cognito

import "context"

func (m *AttemptManager) StartBoundAuthorization(ctx context.Context, browserHash [32]byte) (AuthorizationStart, error) {
	if ctx == nil || browserHash == ([32]byte{}) {
		return AuthorizationStart{}, ErrInvalidAttemptConfig
	}
	return m.startAuthorizationWithBrowserBinding(ctx, browserHash)
}
