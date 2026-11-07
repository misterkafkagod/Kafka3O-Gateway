package cognito

import "time"

func (r CodeExchangeResult) ForBoundSession(protector *RefreshTokenProtector, now time.Time, browserHash [32]byte) (VerifiedCodeExchange, error) {
	exchange := VerifiedCodeExchange{proof: r.proof}
	if _, err := exchange.BoundDetails(protector, now, browserHash); err != nil {
		return VerifiedCodeExchange{}, err
	}
	return exchange, nil
}
