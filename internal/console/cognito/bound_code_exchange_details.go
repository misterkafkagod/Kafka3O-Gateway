package cognito

import "time"

func (e VerifiedCodeExchange) BoundDetails(protector *RefreshTokenProtector, now time.Time, browserHash [32]byte) (SessionExchangeDetails, error) {
	if browserHash == ([32]byte{}) {
		return SessionExchangeDetails{}, ErrCodeExchangeUnavailable
	}
	return e.detailsWithBrowserBinding(protector, now, browserHash)
}
