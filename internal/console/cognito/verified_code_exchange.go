package cognito

import (
	"time"
)

func (r CodeExchangeResult) ForSession(protector *RefreshTokenProtector, now time.Time) (VerifiedCodeExchange, error) {
	exchange := VerifiedCodeExchange{proof: r.proof}
	if _, err := exchange.Details(protector, now); err != nil {
		return VerifiedCodeExchange{}, err
	}
	return exchange, nil
}

func (e VerifiedCodeExchange) Details(protector *RefreshTokenProtector, now time.Time) (SessionExchangeDetails, error) {
	return e.detailsWithBrowserBinding(protector, now, [32]byte{})
}

func minTime(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
