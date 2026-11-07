package cognito

import "context"

func (e *CodeExchanger) RefreshBoundCognitoSession(ctx context.Context, tokenHash, browserHash [32]byte, protector *RefreshTokenProtector) (RefreshExchangeResult, error) {
	if browserHash == ([32]byte{}) {
		return RefreshExchangeResult{}, ErrRefreshState
	}
	return e.refreshCognitoSessionWithBrowserBinding(ctx, tokenHash, browserHash, protector)
}
