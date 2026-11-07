package cognito

import (
	"crypto/subtle"
	"time"
)

func (r VerifiedRefresh) claimRefreshCompletionWithBrowserBinding(protector *RefreshTokenProtector, now time.Time, browserHash [32]byte) (RefreshCompletionDetails, error) {
	if r.proof == nil || subtle.ConstantTimeCompare(r.proof.browserBindingHash[:], browserHash[:]) != 1 {
		return RefreshCompletionDetails{}, ErrRefreshUnavailable
	}
	if browserHash != ([32]byte{}) {
		if !r.proof.completionAttempted.CompareAndSwap(false, true) {
			return RefreshCompletionDetails{}, ErrRefreshUnavailable
		}
		return r.Details(protector, now)
	}
	details, err := r.Details(protector, now)
	if err != nil {
		return RefreshCompletionDetails{}, err
	}
	if !r.proof.completionAttempted.CompareAndSwap(false, true) {
		return RefreshCompletionDetails{}, ErrRefreshUnavailable
	}
	return details, nil
}
