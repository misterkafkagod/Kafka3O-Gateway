package cognito

import "context"

func (e *CodeExchanger) RefreshCognitoSession(ctx context.Context, tokenHash [32]byte, protector *RefreshTokenProtector) (RefreshExchangeResult, error) {
	return e.refreshCognitoSessionWithBrowserBinding(ctx, tokenHash, [32]byte{}, protector)
}
