package cognito

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

func (e *CodeExchanger) ExchangeAuthorizationCode(ctx context.Context, attempt ConsumedAuthorization, code string) (CodeExchangeResult, error) {
	if attempt.proof == nil || attempt.proof.manager != e.attempts {
		return CodeExchangeResult{}, ErrCodeExchangeUnavailable
	}
	proof := attempt.proof
	result := CodeExchangeResult{OperationID: proof.operationID}
	denialID, _, err := e.attempts.generateAttemptEventIDs()
	if err != nil {
		return result, err
	}
	if err := e.store.ReserveCognitoCodeExchange(ctx, CodeExchangeReservation{
		OperationID: proof.operationID, DenialID: denialID, OccurredAt: e.attempts.now().UTC(), ExpiresAt: proof.expiresAt,
	}); err != nil {
		return result, err
	}
	var identity VerifiedIdentity
	var refresh string
	var verifiedAt time.Time
	var exchangeErr error
	if !validAuthorizationCode(code) {
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
	if exchangeErr == nil && identity.ExpiresAt.Before(validUntil) {
		validUntil = identity.ExpiresAt
	}
	if err := e.store.FinishCognitoCodeExchange(ctx, CodeExchangeOutcome{
		OperationID: proof.operationID, OccurredAt: e.attempts.now().UTC(), ValidUntil: validUntil, Status: status,
	}); errors.Is(err, ErrCodeExchangeExpired) {
		return result, errors.Join(exchangeErr, ErrCodeExchangeUnavailable, err)
	} else if err != nil {
		return result, errors.Join(exchangeErr, ErrCodeExchangeAudit, err)
	}
	if exchangeErr != nil {
		return result, exchangeErr
	}
	result.Identity, result.RefreshToken = identity, refresh
	provenIdentity := identity
	provenIdentity.Audience = slices.Clone(identity.Audience)
	provenIdentity.Groups = slices.Clone(identity.Groups)
	result.proof = &verifiedExchangeProof{
		operationID: proof.operationID, identity: provenIdentity, nonceHash: proof.nonceHash,
		refreshToken: refresh, providerBinding: e.attempts.providerBinding,
		verifiedAt: verifiedAt, attemptExpiry: proof.expiresAt,
	}
	return result, nil
}
