package cognito

import (
	"encoding/base64"
	"fmt"
	"io"
)

func (m *AttemptManager) generateAttemptEventIDs() (string, string, error) {
	var entropy [32]byte
	m.mu.Lock()
	_, err := io.ReadFull(m.random, entropy[:])
	m.mu.Unlock()
	if err != nil {
		return "", "", fmt.Errorf("generate Cognito audit identifiers: %w", err)
	}
	first := base64.RawURLEncoding.EncodeToString(entropy[:16])
	second := base64.RawURLEncoding.EncodeToString(entropy[16:])
	if first == second {
		return "", "", ErrInvalidAttemptConfig
	}
	return first, second, nil
}
