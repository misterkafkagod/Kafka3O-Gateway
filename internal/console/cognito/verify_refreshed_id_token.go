package cognito

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"slices"
	"time"
)

func (v *Verifier) VerifyRefreshedIDToken(ctx context.Context, rawToken string,
	previous VerifiedIdentity, originalNonceHash [32]byte) (VerifiedIdentity, error) {
	if previous.Issuer != v.issuer || previous.Subject == "" || previous.IssuedAt.IsZero() ||
		!slices.Contains(previous.Audience, v.clientID) || originalNonceHash == ([32]byte{}) {
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	identity, nonce, present, err := v.verifyIdentityToken(ctx, rawToken)
	if err != nil {
		return VerifiedIdentity{}, err
	}
	now := v.now()
	if identity.Issuer != previous.Issuer || identity.Subject != previous.Subject ||
		!sameAudiences(identity.Audience, previous.Audience) || !identity.IssuedAt.After(previous.IssuedAt) ||
		now.IsZero() || identity.IssuedAt.After(now.Add(5*time.Minute)) ||
		!identity.IssuedAt.Add(5*time.Minute).After(now) || !identity.ExpiresAt.After(now) {
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	if present {
		digest := sha256.Sum256([]byte(nonce))
		if subtle.ConstantTimeCompare(digest[:], originalNonceHash[:]) != 1 {
			return VerifiedIdentity{}, ErrInvalidIDToken
		}
	}
	return identity, nil
}

func sameAudiences(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	if slices.Equal(first, second) {
		return true
	}
	first = slices.Clone(first)
	second = slices.Clone(second)
	slices.Sort(first)
	slices.Sort(second)
	return slices.Equal(first, second)
}
