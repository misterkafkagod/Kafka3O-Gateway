package cognito

import (
	"crypto/subtle"
	"time"
)

func (m *AttemptManager) validateBrowserAuthorization(start AuthorizationStart) (time.Time, error) {
	proof := start.proof
	if m == nil || proof == nil || proof.manager != m || proof.operationID != start.OperationID ||
		!proof.expiresAt.Equal(start.ExpiresAt) {
		return time.Time{}, ErrInvalidAttempt
	}
	now := m.now().UTC()
	if now.IsZero() || now.Before(proof.createdAt) || !now.Before(proof.expiresAt) ||
		!proof.expiresAt.Equal(proof.createdAt.Add(AttemptLifetime)) {
		return time.Time{}, ErrInvalidAttempt
	}
	for i, value := range []string{start.State, start.Nonce, start.CodeChallenge, start.BrowserBinding} {
		hash, err := hashAttemptValue(value)
		if err != nil || subtle.ConstantTimeCompare(hash[:], proof.valueHashes[i][:]) != 1 {
			return time.Time{}, ErrInvalidAttempt
		}
	}
	return now, nil
}
