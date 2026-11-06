package cognito

import (
	"context"
	"errors"
)

func (m *AttemptManager) ConsumeAuthorization(ctx context.Context, state, browserBinding string) (ConsumedAuthorization, error) {
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
	record, err := m.store.ConsumeCognitoAttempt(ctx, AttemptConsumption{
		EventID: eventID, StateHash: stateHash, BindingHash: bindingHash,
		Valid: stateErr == nil && bindingErr == nil, OccurredAt: now,
	})
	result := ConsumedAuthorization{OperationID: record.OperationID}
	if err != nil {
		return result, err
	}
	additional := attemptAdditionalData(stateHash, bindingHash, record.NonceHash, m.providerBinding)
	if len(record.EncryptedPKCE) >= m.aead.NonceSize()+m.aead.Overhead() {
		plaintext, err := m.aead.Open(nil, record.EncryptedPKCE[:m.aead.NonceSize()],
			record.EncryptedPKCE[m.aead.NonceSize():], additional[:])
		if err == nil {
			verifier := string(plaintext)
			if _, validationErr := hashAttemptValue(verifier); validationErr == nil {
				result.NonceHash, result.CodeVerifier = record.NonceHash, verifier
				result.proof = &consumedAttemptProof{
					manager: m, operationID: record.OperationID, nonceHash: record.NonceHash, codeVerifier: verifier,
					expiresAt: record.ExpiresAt,
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
