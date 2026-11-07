package cognito

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
)

func (m *AttemptManager) startAuthorizationWithBrowserBinding(ctx context.Context, browserHash [32]byte) (AuthorizationStart, error) {
	now := m.now().UTC()
	expires := now.Add(AttemptLifetime)
	if now.IsZero() || now.Year() < 1970 || expires.Year() > 2261 || !expires.After(now) {
		return AuthorizationStart{}, ErrInvalidAttemptConfig
	}
	secrets, err := m.generateAttemptSecrets()
	if err != nil {
		return AuthorizationStart{}, err
	}
	result := AuthorizationStart{OperationID: secrets.operationID}
	stateHash := sha256.Sum256([]byte(secrets.state))
	transientHash := sha256.Sum256([]byte(secrets.binding))
	bindingHash := transientHash
	if browserHash != ([32]byte{}) {
		bindingHash = stableAttemptBindingHash(transientHash, browserHash)
	}
	nonceHash := sha256.Sum256([]byte(secrets.nonce))
	verifierHash := sha256.Sum256([]byte(secrets.verifier))
	baseAdditional := attemptAdditionalData(stateHash, bindingHash, nonceHash, m.providerBinding)
	additional := baseAdditional[:]
	if browserHash != ([32]byte{}) {
		boundAdditional := stableAttemptAdditionalData(stateHash, bindingHash, nonceHash, m.providerBinding, secrets.operationID, expires)
		additional = boundAdditional[:]
	}
	encrypted := m.aead.Seal(secrets.cryptoNonce[:], secrets.cryptoNonce[:], []byte(secrets.verifier), additional)
	createdAt, err := m.store.CreateCognitoAttempt(ctx, AttemptRecord{
		OperationID: secrets.operationID, StateHash: stateHash, BindingHash: bindingHash,
		NonceHash: nonceHash, EncryptedPKCE: encrypted, Bound: browserHash != ([32]byte{}),
		OccurredAt: now, ExpiresAt: expires,
	})
	if err != nil {
		return result, err
	}
	if browserHash != ([32]byte{}) {
		completedAt := m.now().UTC()
		if createdAt.Before(now) || completedAt.Before(createdAt) || completedAt.Year() > 2261 || !expires.After(completedAt) {
			return result, ErrAttemptUnknown
		}
	}
	challenge := base64.RawURLEncoding.EncodeToString(verifierHash[:])
	return AuthorizationStart{
		OperationID: secrets.operationID, State: secrets.state, Nonce: secrets.nonce,
		CodeChallenge: challenge, BrowserBinding: secrets.binding, ExpiresAt: expires,
		proof: &authorizationStartProof{
			manager: m, operationID: secrets.operationID,
			valueHashes: [4][32]byte{stateHash, nonceHash, sha256.Sum256([]byte(challenge)), transientHash},
			createdAt:   now, expiresAt: expires,
		},
	}, nil
}
