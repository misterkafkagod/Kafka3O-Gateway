package cognito

import (
	"crypto/subtle"
	"time"
)

func (a ConsumedAuthorization) MatchesCodeExchangeReservation(operationID string, expiresAt, requestedAt time.Time, browserHash [32]byte) bool {
	proof := a.proof
	return proof != nil && proof.operationID == operationID && proof.expiresAt.Equal(expiresAt) &&
		(browserHash == ([32]byte{}) || (!proof.consumedAt.IsZero() && !requestedAt.Before(proof.consumedAt))) &&
		subtle.ConstantTimeCompare(proof.browserBindingHash[:], browserHash[:]) == 1
}
