package cognito

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

func (e *CodeExchanger) exchangeAuthorizationCodeWithBrowserBinding(ctx context.Context, attempt ConsumedAuthorization, code string, browserHash [32]byte) (CodeExchangeResult, error) {
	proof := attempt.proof
	result := CodeExchangeResult{OperationID: proof.operationID}
	denialID, _, err := e.attempts.generateAttemptEventIDs()
	if err != nil {
		return result, err
	}
	requestedAt := e.attempts.now().UTC()
	bound := browserHash != ([32]byte{})
	if bound && (proof.consumedAt.IsZero() || requestedAt.Before(proof.consumedAt) || requestedAt.Year() > 2261) {
		return result, ErrCodeExchangeUnknown
	}
	reservedAt, err := e.store.ReserveCognitoCodeExchange(ctx, CodeExchangeReservation{
		OperationID: proof.operationID, DenialID: denialID, Attempt: attempt, BrowserBindingHash: browserHash,
		OccurredAt: requestedAt, ExpiresAt: proof.expiresAt,
	})
	if err != nil {
		return result, err
	}
	var identity VerifiedIdentity
	var refresh string
	var verifiedAt time.Time
	var exchangeErr error
	sendAt := e.attempts.now().UTC()
	if bound && (reservedAt.Before(requestedAt) || sendAt.Before(reservedAt) || sendAt.Year() > 2261) {
		return result, ErrCodeExchangeUnknown
	}
	if bound && !proof.expiresAt.After(sendAt) {
		exchangeErr = ErrCodeExchangeUnavailable
	} else if !validAuthorizationCode(code) {
		exchangeErr = ErrCodeExchangeUnavailable
	} else {
		token, err := e.oauth.Exchange(oidc.ClientContext(ctx, e.client), code, oauth2.VerifierOption(proof.codeVerifier))
		if err != nil {
			exchangeErr = ErrCodeExchangeUnknown
		} else {
			rawID, ok := token.Extra("id_token").(string)
			if !ok || !validRefreshToken(token.RefreshToken) {
				exchangeErr = ErrCodeExchangeUnknown
			} else {
				identity, exchangeErr = e.verifier.VerifyIDToken(ctx, rawID, proof.nonceHash)
				if exchangeErr == nil {
					refresh = token.RefreshToken
					verifiedAt = e.attempts.now().UTC()
					if bound && (verifiedAt.Before(sendAt) || verifiedAt.Year() > 2261) {
						return result, ErrCodeExchangeUnknown
					}
				}
			}
		}
	}
	status := CodeExchangeFailed
	if exchangeErr == nil {
		status = CodeExchangeSucceeded
	} else if errors.Is(exchangeErr, ErrCodeExchangeUnknown) {
		status = CodeExchangeUnknown
	}
	validUntil := proof.expiresAt
	if exchangeErr == nil {
		validUntil = minTime(validUntil, identity.ExpiresAt)
		if bound {
			validUntil = minTime(validUntil, identity.IssuedAt.Add(5*time.Minute))
			validUntil = minTime(validUntil, verifiedAt.Add(5*time.Minute))
		}
	}
	finishAt := e.attempts.now().UTC()
	if bound && (finishAt.Before(sendAt) || finishAt.Year() > 2261 || (exchangeErr == nil && finishAt.Before(verifiedAt))) {
		return result, ErrCodeExchangeUnknown
	}
	completedAt, err := e.store.FinishCognitoCodeExchange(ctx, CodeExchangeOutcome{
		OperationID: proof.operationID, OccurredAt: finishAt, ValidUntil: validUntil, Status: status, Bound: bound,
	})
	if errors.Is(err, ErrCodeExchangeExpired) {
		return result, errors.Join(exchangeErr, ErrCodeExchangeUnavailable, err)
	} else if err != nil {
		return result, errors.Join(exchangeErr, ErrCodeExchangeAudit, err)
	}
	if exchangeErr != nil {
		return result, exchangeErr
	}
	if bound {
		returnedAt := e.attempts.now().UTC()
		if completedAt.Before(finishAt) || returnedAt.Before(completedAt) || returnedAt.Year() > 2261 || !validUntil.After(returnedAt) {
			return result, ErrCodeExchangeUnknown
		}
	}
	result.Identity, result.RefreshToken = identity, refresh
	provenIdentity := identity
	provenIdentity.Audience = slices.Clone(identity.Audience)
	provenIdentity.Groups = slices.Clone(identity.Groups)
	result.proof = &verifiedExchangeProof{
		operationID: proof.operationID, identity: provenIdentity, nonceHash: proof.nonceHash,
		refreshToken: refresh, providerBinding: e.attempts.providerBinding, browserBindingHash: browserHash,
		verifiedAt: verifiedAt, attemptExpiry: proof.expiresAt,
	}
	return result, nil
}
