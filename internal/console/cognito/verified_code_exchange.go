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
