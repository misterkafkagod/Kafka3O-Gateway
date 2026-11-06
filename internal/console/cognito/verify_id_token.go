package cognito

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"time"
)

func (v *Verifier) VerifyIDToken(ctx context.Context, rawToken string, nonceHash [32]byte) (VerifiedIdentity, error) {
	if ctx == nil || rawToken == "" || len(rawToken) > maxIDTokenBytes || nonceHash == ([32]byte{}) {
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	if err := ctx.Err(); err != nil {
		return VerifiedIdentity{}, err
	}
	attempt := &verificationAttempt{}
	token, err := v.verifier.Verify(context.WithValue(ctx, verificationAttemptKey{}, attempt), rawToken)
	if err != nil {
		if ctx.Err() != nil {
			return VerifiedIdentity{}, ctx.Err()
		}
		// The pinned RemoteKeySet wraps JWKS fetch/decode failures, but not a
		// signature mismatch; tests enforce this distinction on upgrades.
		if errors.Is(attempt.keyError, ErrProviderUnavailable) || errors.Unwrap(attempt.keyError) != nil {
			return VerifiedIdentity{}, ErrProviderUnavailable
		}
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	var claims struct {
		TokenUse        string   `json:"token_use"`
		AuthorizedParty string   `json:"azp"`
		Groups          []string `json:"cognito:groups"`
	}
	now := v.now()
	nonce := sha256.Sum256([]byte(token.Nonce))
	if err := token.Claims(&claims); err != nil || token.Issuer != v.issuer ||
		claims.TokenUse != "id" || token.Subject == "" ||
		token.Nonce == "" || subtle.ConstantTimeCompare(nonce[:], nonceHash[:]) != 1 ||
		now.IsZero() || token.IssuedAt.IsZero() || token.IssuedAt.After(now.Add(5*time.Minute)) ||
		!token.IssuedAt.Before(token.Expiry) || !token.Expiry.After(now) ||
		(claims.AuthorizedParty != "" && claims.AuthorizedParty != v.clientID) ||
		(len(token.Audience) > 1 && claims.AuthorizedParty != v.clientID) {
		return VerifiedIdentity{}, ErrInvalidIDToken
	}
	return VerifiedIdentity{
		Issuer: token.Issuer, Subject: token.Subject, Groups: claims.Groups, ExpiresAt: token.Expiry.UTC(),
	}, nil
}
