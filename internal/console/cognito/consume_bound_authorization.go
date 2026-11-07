package cognito

import "context"

func (m *AttemptManager) ConsumeBoundAuthorization(ctx context.Context, state, browserBinding string, browserHash [32]byte) (ConsumedAuthorization, error) {
	if ctx == nil {
		return ConsumedAuthorization{}, ErrInvalidAttemptConfig
	}
	return m.consumeAuthorizationWithBrowserBinding(ctx, state, browserBinding, browserHash, true)
}
