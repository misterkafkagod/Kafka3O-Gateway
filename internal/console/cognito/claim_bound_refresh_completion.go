package cognito

import "time"

func (r VerifiedRefresh) ClaimBound(protector *RefreshTokenProtector, now time.Time, browserHash [32]byte) (RefreshCompletionDetails, error) {
	if browserHash == ([32]byte{}) {
		return RefreshCompletionDetails{}, ErrRefreshUnavailable
	}
	return r.claimRefreshCompletionWithBrowserBinding(protector, now, browserHash)
}
