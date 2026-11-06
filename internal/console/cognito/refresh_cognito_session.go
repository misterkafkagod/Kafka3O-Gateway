package cognito

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

func (e *CodeExchanger) RefreshCognitoSession(ctx context.Context, tokenHash [32]byte, protector *RefreshTokenProtector) (RefreshExchangeResult, error) {
	if ctx == nil || tokenHash == ([32]byte{}) || !e.SupportsSessionProtector(protector) {
		return RefreshExchangeResult{}, ErrRefreshState
	}
	if err := ctx.Err(); err != nil {
		return RefreshExchangeResult{}, err
	}
	store, ok := e.attempts.store.(RefreshStore)
	if !ok {
		return RefreshExchangeResult{}, ErrRefreshState
	}
	operationID, _, err := e.attempts.generateAttemptEventIDs()
	if err != nil {
		return RefreshExchangeResult{}, err
	}
	failureResult := RefreshExchangeResult{OperationID: operationID}
	reservation, err := store.ReserveCognitoRefresh(ctx, RefreshReservationRequest{
		OperationID: operationID, TokenHash: tokenHash, Protector: protector, OccurredAt: e.attempts.now().UTC(),
	})
	if err != nil {
		return failureResult, err
	}
	if reservation.OperationID != operationID || reservation.SessionID == "" || reservation.IdentityID == "" || reservation.Generation < 1 {
		return failureResult, ErrRefreshState
	}
	fail := func(cause error, status RefreshFailureStatus) (RefreshExchangeResult, error) {
		auditErr := store.RecordCognitoRefreshFailure(ctx, RefreshFailure{
			OperationID: operationID, SessionID: reservation.SessionID, Generation: reservation.Generation,
			OccurredAt: e.attempts.now().UTC(), Status: status,
		})
		if auditErr != nil {
			if errors.Is(auditErr, ErrRefreshUnknown) {
				return failureResult, auditErr
			}
			return failureResult, errors.Join(ErrRefreshAudit, auditErr)
		}
		return failureResult, cause
	}
	now := e.attempts.now().UTC()
	previous := reservation.PreviousIdentity
	if now.IsZero() || now.Year() < 1970 || now.Year() > 2261 || reservation.Generation == math.MaxInt64 ||
		previous.Issuer != e.verifier.issuer || !ValidSubject(previous.Subject) ||
		!slices.Contains(previous.Audience, e.verifier.clientID) || previous.IssuedAt.IsZero() ||
		previous.IssuedAt.Year() < 1970 || previous.IssuedAt.Year() > 2261 ||
		!previous.IssuedAt.Before(previous.ExpiresAt) || reservation.OriginalNonceHash == ([32]byte{}) {
		return fail(ErrRefreshState, RefreshFailedBeforeSend)
	}
	if !reservation.IdleExpiresAt.After(now) || !reservation.AbsoluteExpiresAt.After(now) {
		return fail(ErrRefreshUnavailable, RefreshFailedBeforeSend)
	}
	binding := RefreshTokenBinding{SessionID: reservation.SessionID, IdentityID: reservation.IdentityID, Generation: reservation.Generation}
	oldToken, err := protector.Open(binding, reservation.EncryptedRefreshToken)
	if err != nil {
		return fail(err, RefreshFailedBeforeSend)
	}
	if err := ctx.Err(); err != nil {
		return fail(err, RefreshFailedBeforeSend)
	}
	sendAt := e.attempts.now().UTC()
	if sendAt.Before(now) || sendAt.Year() > 2261 {
		return fail(ErrRefreshState, RefreshFailedBeforeSend)
	}
	if !reservation.IdleExpiresAt.After(sendAt) || !reservation.AbsoluteExpiresAt.After(sendAt) {
		return fail(ErrRefreshUnavailable, RefreshFailedBeforeSend)
	}
	token, err := e.oauth.TokenSource(oidc.ClientContext(ctx, e.client), &oauth2.Token{RefreshToken: oldToken}).Token()
	if err != nil {
		return fail(ErrRefreshUnknown, RefreshOutcomeUnknown)
	}
	rawID, ok := token.Extra("id_token").(string)
	if !ok || !validRefreshToken(token.RefreshToken) {
		return fail(ErrInvalidIDToken, RefreshOutcomeUnknown)
	}
	identity, err := e.verifier.VerifyRefreshedIDToken(ctx, rawID, previous, reservation.OriginalNonceHash)
	if err != nil {
		return fail(err, RefreshOutcomeUnknown)
	}
	verifiedAt := e.attempts.now().UTC()
	if verifiedAt.IsZero() || verifiedAt.Year() < 1970 || verifiedAt.Year() > 2261 ||
		identity.IssuedAt.Year() > 2261 || identity.ExpiresAt.Year() > 2261 || verifiedAt.Before(sendAt) {
		return fail(ErrRefreshState, RefreshOutcomeUnknown)
	}
	freshUntil := identity.ExpiresAt
	for _, deadline := range []time.Time{identity.IssuedAt.Add(5 * time.Minute), verifiedAt.Add(5 * time.Minute)} {
		freshUntil = minTime(freshUntil, deadline)
	}
	binding.Generation++
	encrypted, err := protector.Seal(binding, token.RefreshToken)
	if err != nil {
		return fail(err, RefreshOutcomeUnknown)
	}
	identity.Audience = slices.Clone(identity.Audience)
	identity.Groups = slices.Clone(identity.Groups)
	result := RefreshExchangeResult{
		OperationID: operationID,
		proof: &verifiedRefreshProof{
			operationID: operationID, sessionID: reservation.SessionID, identityID: reservation.IdentityID,
			previousGeneration: reservation.Generation, identity: identity, nonceHash: reservation.OriginalNonceHash,
			encryptedRefreshToken: encrypted, providerBinding: protector.providerBinding, verifiedAt: verifiedAt,
			claimsFreshUntil: freshUntil, sessionDeadline: minTime(reservation.IdleExpiresAt, reservation.AbsoluteExpiresAt),
		},
	}
	if _, err := result.ForCompletion(protector, e.attempts.now().UTC()); err != nil {
		return fail(err, RefreshOutcomeUnknown)
	}
	return result, nil
}
