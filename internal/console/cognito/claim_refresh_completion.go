package cognito

import "time"

func (r VerifiedRefresh) Claim(protector *RefreshTokenProtector, now time.Time) (RefreshCompletionDetails, error) {
	return r.claimRefreshCompletionWithBrowserBinding(protector, now, [32]byte{})
}
