package cognito

import (
	"encoding/base64"
	"fmt"
	"io"
)

func (m *AttemptManager) generateAttemptSecrets() (attemptSecrets, error) {
	var entropy [156]byte
	m.mu.Lock()
	_, err := io.ReadFull(m.random, entropy[:])
	m.mu.Unlock()
	if err != nil {
		return attemptSecrets{}, fmt.Errorf("generate Cognito authorization values: %w", err)
	}
	secrets := attemptSecrets{
		operationID: base64.RawURLEncoding.EncodeToString(entropy[:16]),
		state:       base64.RawURLEncoding.EncodeToString(entropy[16:48]),
		nonce:       base64.RawURLEncoding.EncodeToString(entropy[48:80]),
		verifier:    base64.RawURLEncoding.EncodeToString(entropy[80:112]),
		binding:     base64.RawURLEncoding.EncodeToString(entropy[112:144]),
	}
	copy(secrets.cryptoNonce[:], entropy[144:])
	if secrets.state == secrets.nonce || secrets.state == secrets.verifier ||
		secrets.state == secrets.binding || secrets.nonce == secrets.verifier ||
		secrets.nonce == secrets.binding || secrets.verifier == secrets.binding {
		return attemptSecrets{}, ErrInvalidAttemptConfig
	}
	return secrets, nil
}
