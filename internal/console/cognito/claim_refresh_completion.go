package cognito

import "time"

func (r VerifiedRefresh) Claim(protector *RefreshTokenProtector, now time.Time) (RefreshCompletionDetails, error) {
	details, err := r.Details(protector, now)
	if err != nil {
		return RefreshCompletionDetails{}, err
	}
	if !r.proof.completionAttempted.CompareAndSwap(false, true) {
		return RefreshCompletionDetails{}, ErrRefreshUnavailable
	}
	return details, nil
}
