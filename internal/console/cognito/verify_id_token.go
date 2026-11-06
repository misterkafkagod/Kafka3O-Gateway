package cognito

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
)

func (v *Verifier) VerifyIDToken(ctx context.Context, rawToken string, nonceHash [32]byte) (VerifiedIdentity, error) {
	if nonceHash == ([32]byte{}) {
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	identity, nonce, present, err := v.verifyIdentityToken(ctx, rawToken)
	if err != nil {
		return VerifiedIdentity{}, err
	}
	digest := sha256.Sum256([]byte(nonce))
	if !present || subtle.ConstantTimeCompare(digest[:], nonceHash[:]) != 1 {
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	return identity, nil
}
