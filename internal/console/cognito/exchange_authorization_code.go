package cognito

import (
	"context"
)

func (e *CodeExchanger) ExchangeAuthorizationCode(ctx context.Context, attempt ConsumedAuthorization, code string) (CodeExchangeResult, error) {
	if e == nil || ctx == nil || attempt.proof == nil || attempt.proof.manager != e.attempts ||
		attempt.proof.browserBindingHash != ([32]byte{}) {
		return CodeExchangeResult{}, ErrCodeExchangeUnavailable
	}
	return e.exchangeAuthorizationCodeWithBrowserBinding(ctx, attempt, code, [32]byte{})
}
