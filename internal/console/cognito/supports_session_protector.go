package cognito

import "crypto/subtle"

func (e *CodeExchanger) SupportsSessionProtector(protector *RefreshTokenProtector) bool {
	if e == nil || e.attempts == nil || e.verifier == nil || protector == nil {
		return false
	}
	return subtle.ConstantTimeCompare(e.attempts.providerBinding[:], protector.providerBinding[:]) == 1
}
