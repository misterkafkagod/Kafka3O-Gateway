package cognito

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

func (v *Verifier) verifyIdentityToken(ctx context.Context, rawToken string) (VerifiedIdentity, string, bool, error) {
	if ctx == nil || rawToken == "" || len(rawToken) > maxIDTokenBytes {
		return VerifiedIdentity{}, "", false, ErrInvalidIDToken
	}
	if err := ctx.Err(); err != nil {
		return VerifiedIdentity{}, "", false, err
	}
	attempt := &verificationAttempt{}
	token, err := v.verifier.Verify(context.WithValue(ctx, verificationAttemptKey{}, attempt), rawToken)
	if err != nil {
		if ctx.Err() != nil {
			return VerifiedIdentity{}, "", false, ctx.Err()
		}
		// The pinned RemoteKeySet wraps JWKS fetch/decode failures, but not a
		// signature mismatch; tests enforce this distinction on upgrades.
		if errors.Is(attempt.keyError, ErrProviderUnavailable) || errors.Unwrap(attempt.keyError) != nil {
			return VerifiedIdentity{}, "", false, ErrProviderUnavailable
		}
		return VerifiedIdentity{}, "", false, ErrInvalidIDToken
	}
	var claims struct {
		TokenUse        string          `json:"token_use"`
		AuthorizedParty string          `json:"azp"`
		Groups          json.RawMessage `json:"cognito:groups"`
		Nonce           json.RawMessage `json:"nonce"`
	}
	now := v.now()
	if err := token.Claims(&claims); err != nil || token.Issuer != v.issuer ||
		claims.TokenUse != "id" || !validSubject(token.Subject) ||
		now.IsZero() || token.IssuedAt.IsZero() || token.IssuedAt.After(now.Add(5*time.Minute)) ||
		!token.IssuedAt.Before(token.Expiry) || !token.Expiry.After(now) ||
		(claims.AuthorizedParty != "" && claims.AuthorizedParty != v.clientID) ||
		(len(token.Audience) > 1 && claims.AuthorizedParty != v.clientID) {
		return VerifiedIdentity{}, "", false, ErrInvalidIDToken
	}

	var groups []string
	if claims.Groups != nil {
		var values []json.RawMessage
		if json.Unmarshal(claims.Groups, &values) != nil || values == nil {
			return VerifiedIdentity{}, "", false, ErrInvalidIDToken
		}
		groups = make([]string, len(values))
		for i, value := range values {
			if json.Unmarshal(value, &groups[i]) != nil || groups[i] == "" {
				return VerifiedIdentity{}, "", false, ErrInvalidIDToken
			}
		}
	}
	var nonce string
	if claims.Nonce != nil && (json.Unmarshal(claims.Nonce, &nonce) != nil || nonce == "") {
		return VerifiedIdentity{}, "", false, ErrInvalidIDToken
	}
	return VerifiedIdentity{
		Issuer: token.Issuer, Subject: token.Subject, Audience: token.Audience,
		Groups: groups, IssuedAt: token.IssuedAt.UTC(), ExpiresAt: token.Expiry.UTC(),
	}, nonce, claims.Nonce != nil, nil
}
