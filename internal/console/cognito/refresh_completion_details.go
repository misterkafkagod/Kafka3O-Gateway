package cognito

import (
	"crypto/subtle"
	"math"
	"slices"
	"time"
)

func (r VerifiedRefresh) Details(protector *RefreshTokenProtector, now time.Time) (RefreshCompletionDetails, error) {
	proof := r.proof
	if proof == nil || protector == nil || now.IsZero() || now.Year() < 1970 || now.Year() > 2261 ||
		proof.verifiedAt.IsZero() || now.Before(proof.verifiedAt) || proof.previousGeneration < 1 ||
		proof.previousGeneration == math.MaxInt64 || proof.nonceHash == ([32]byte{}) ||
		subtle.ConstantTimeCompare(proof.providerBinding[:], protector.providerBinding[:]) != 1 {
		return RefreshCompletionDetails{}, ErrRefreshUnavailable
	}
	validUntil := minTime(proof.claimsFreshUntil, proof.sessionDeadline)
	if !validUntil.After(now) {
		return RefreshCompletionDetails{}, ErrRefreshUnavailable
	}
	binding := RefreshTokenBinding{
		SessionID: proof.sessionID, IdentityID: proof.identityID, Generation: proof.previousGeneration + 1,
	}
	if _, err := protector.Open(binding, proof.encryptedRefreshToken); err != nil {
		return RefreshCompletionDetails{}, err
	}
	identity := proof.identity
	identity.Audience = slices.Clone(identity.Audience)
	identity.Groups = slices.Clone(identity.Groups)
	previous := proof.previousIdentity
	previous.Audience = slices.Clone(previous.Audience)
	previous.Groups = slices.Clone(previous.Groups)
	return RefreshCompletionDetails{
		OperationID: proof.operationID, SessionID: proof.sessionID, IdentityID: proof.identityID,
		PreviousGeneration: proof.previousGeneration, PreviousIdentity: previous,
		PreviousClaimsFreshUntil: proof.previousClaimsFreshUntil, PreviousEncryptedHash: proof.previousEncryptedHash,
		Identity: identity, OriginalNonceHash: proof.nonceHash,
		EncryptedRefreshToken: slices.Clone(proof.encryptedRefreshToken), ClaimsFreshUntil: proof.claimsFreshUntil,
		ValidUntil: validUntil,
	}, nil
}
