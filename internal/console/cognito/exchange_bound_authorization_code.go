package cognito

import (
	"context"
	"crypto/subtle"
)

func (e *CodeExchanger) ExchangeBoundAuthorizationCode(ctx context.Context, attempt ConsumedAuthorization, code string, browserHash [32]byte) (CodeExchangeResult, error) {
	if e == nil || ctx == nil || browserHash == ([32]byte{}) || attempt.proof == nil ||
		attempt.proof.manager != e.attempts ||
		subtle.ConstantTimeCompare(attempt.proof.browserBindingHash[:], browserHash[:]) != 1 {
		return CodeExchangeResult{}, ErrCodeExchangeUnavailable
	}
	return e.exchangeAuthorizationCodeWithBrowserBinding(ctx, attempt, code, browserHash)
}
