package cognito

import (
	"time"
)

func (e VerifiedCodeExchange) Details(protector *RefreshTokenProtector, now time.Time) (SessionExchangeDetails, error) {
	return e.detailsWithBrowserBinding(protector, now, [32]byte{})
}
