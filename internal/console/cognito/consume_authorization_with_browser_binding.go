package cognito

import (
	"context"
	"errors"
)

func (m *AttemptManager) consumeAuthorizationWithBrowserBinding(ctx context.Context, state, browserBinding string, browserHash [32]byte, bound bool) (ConsumedAuthorization, error) {
	eventID, failureID, err := m.generateAttemptEventIDs()
	if err != nil {
		return ConsumedAuthorization{}, err
	}
	now := m.now().UTC()
	if now.IsZero() || now.Year() < 1970 || now.Year() > 2261 {
		return ConsumedAuthorization{}, ErrInvalidAttemptConfig
	}
	stateHash, stateErr := hashAttemptValue(state)
	bindingHash, bindingErr := hashAttemptValue(browserBinding)
	if bound {
		bindingHash = stableAttemptBindingHash(bindingHash, browserHash)
	}
	valid := stateErr == nil && bindingErr == nil && (!bound || browserHash != ([32]byte{}))
	record, err := m.store.ConsumeCognitoAttempt(ctx, AttemptConsumption{
		EventID: eventID, StateHash: stateHash, BindingHash: bindingHash,
		Valid: valid, Bound: bound, OccurredAt: now,
	})
	result := ConsumedAuthorization{OperationID: record.OperationID}
	if err != nil {
		return result, err
	}
	if !valid {
		return result, ErrAttemptUnknown
	}
	checkedAt := now
	if bound {
		checkedAt = m.now().UTC()
		if record.OccurredAt.Before(now) || checkedAt.Before(record.OccurredAt) ||
			checkedAt.Year() > 2261 || !record.ExpiresAt.After(checkedAt) {
			return result, ErrAttemptUnknown
		}
	}
	baseAdditional := attemptAdditionalData(stateHash, bindingHash, record.NonceHash, m.providerBinding)
	additional := baseAdditional[:]
	if bound {
		boundAdditional := stableAttemptAdditionalData(stateHash, bindingHash, record.NonceHash, m.providerBinding, record.OperationID, record.ExpiresAt)
		additional = boundAdditional[:]
	}
	if len(record.EncryptedPKCE) >= m.aead.NonceSize()+m.aead.Overhead() {
		plaintext, err := m.aead.Open(nil, record.EncryptedPKCE[:m.aead.NonceSize()],
			record.EncryptedPKCE[m.aead.NonceSize():], additional)
		if err == nil {
			verifier := string(plaintext)
			if _, validationErr := hashAttemptValue(verifier); validationErr == nil {
				if bound {
					completedAt := m.now().UTC()
					if completedAt.Before(checkedAt) || completedAt.Year() > 2261 || !record.ExpiresAt.After(completedAt) {
						return result, ErrAttemptUnknown
					}
				}
				result.NonceHash, result.CodeVerifier = record.NonceHash, verifier
				result.proof = &consumedAttemptProof{
					manager: m, operationID: record.OperationID, nonceHash: record.NonceHash, browserBindingHash: browserHash,
					codeVerifier: verifier, expiresAt: record.ExpiresAt,
				}
				return result, nil
			}
		}
	}
	failure := AttemptFailure{EventID: failureID, OperationID: record.OperationID, OccurredAt: m.now().UTC()}
	if auditErr := m.store.FailCognitoAttempt(ctx, failure); auditErr != nil {
		return result, errors.Join(ErrAttemptEncryption, ErrAttemptAudit, auditErr)
	}
	return result, ErrAttemptEncryption
}
