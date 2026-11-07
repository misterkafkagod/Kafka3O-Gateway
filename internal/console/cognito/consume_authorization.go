package cognito

import "context"

func (m *AttemptManager) ConsumeAuthorization(ctx context.Context, state, browserBinding string) (ConsumedAuthorization, error) {
	return m.consumeAuthorizationWithBrowserBinding(ctx, state, browserBinding, [32]byte{}, false)
}
