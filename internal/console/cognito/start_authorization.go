package cognito

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
)

func (m *AttemptManager) StartAuthorization(ctx context.Context) (AuthorizationStart, error) {
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
	bindingHash := sha256.Sum256([]byte(secrets.binding))
	nonceHash := sha256.Sum256([]byte(secrets.nonce))
	verifierHash := sha256.Sum256([]byte(secrets.verifier))
	additional := attemptAdditionalData(stateHash, bindingHash, nonceHash, m.providerBinding)
	encrypted := m.aead.Seal(secrets.cryptoNonce[:], secrets.cryptoNonce[:], []byte(secrets.verifier), additional[:])
	if err := m.store.CreateCognitoAttempt(ctx, AttemptRecord{
		OperationID: secrets.operationID, StateHash: stateHash, BindingHash: bindingHash,
		NonceHash: nonceHash, EncryptedPKCE: encrypted, OccurredAt: now, ExpiresAt: expires,
	}); err != nil {
		return result, err
	}
	challenge := base64.RawURLEncoding.EncodeToString(verifierHash[:])
	return AuthorizationStart{
		OperationID: secrets.operationID, State: secrets.state, Nonce: secrets.nonce,
		CodeChallenge: challenge, BrowserBinding: secrets.binding, ExpiresAt: expires,
		proof: &authorizationStartProof{
			manager: m, operationID: secrets.operationID,
			valueHashes: [4][32]byte{stateHash, nonceHash, sha256.Sum256([]byte(challenge)), bindingHash},
			createdAt:   now, expiresAt: expires,
		},
	}, nil
}
