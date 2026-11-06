package cognito

import "time"

func (r RefreshExchangeResult) ForCompletion(protector *RefreshTokenProtector, now time.Time) (VerifiedRefresh, error) {
	refresh := VerifiedRefresh{proof: r.proof}
	if _, err := refresh.Details(protector, now); err != nil {
		return VerifiedRefresh{}, err
	}
	return refresh, nil
}
