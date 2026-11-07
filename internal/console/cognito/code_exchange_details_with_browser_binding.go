package cognito

import (
	"crypto/subtle"
	"slices"
	"time"
)

func (e VerifiedCodeExchange) detailsWithBrowserBinding(protector *RefreshTokenProtector, now time.Time, browserHash [32]byte) (SessionExchangeDetails, error) {
	proof := e.proof
	if proof == nil || protector == nil || now.IsZero() || now.Year() < 1970 || now.Year() > 2261 ||
		proof.verifiedAt.IsZero() || now.Before(proof.verifiedAt) || proof.identity.IssuedAt.IsZero() ||
		proof.nonceHash == ([32]byte{}) ||
		subtle.ConstantTimeCompare(proof.browserBindingHash[:], browserHash[:]) != 1 ||
		subtle.ConstantTimeCompare(proof.providerBinding[:], protector.providerBinding[:]) != 1 {
		return SessionExchangeDetails{}, ErrCodeExchangeUnavailable
	}
	freshUntil := proof.identity.ExpiresAt
	for _, deadline := range []time.Time{
		proof.identity.IssuedAt.Add(5 * time.Minute), proof.verifiedAt.Add(5 * time.Minute),
	} {
		if deadline.Before(freshUntil) {
			freshUntil = deadline
		}
	}
	validUntil := minTime(freshUntil, proof.attemptExpiry)
	if !validUntil.After(now) {
		return SessionExchangeDetails{}, ErrCodeExchangeExpired
	}
	identity := proof.identity
	identity.Audience = slices.Clone(identity.Audience)
	identity.Groups = slices.Clone(identity.Groups)
	return SessionExchangeDetails{
		OperationID: proof.operationID, Identity: identity, NonceHash: proof.nonceHash,
		RefreshToken: proof.refreshToken, ValidUntil: validUntil, ClaimsFreshUntil: freshUntil,
	}, nil
}
