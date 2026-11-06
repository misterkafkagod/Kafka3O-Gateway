package cognito

import (
	"crypto/sha256"
	"encoding/base64"
)

func hashAttemptValue(value string) ([32]byte, error) {
	if len(value) != 43 {
		return [32]byte{}, ErrInvalidAttempt
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return [32]byte{}, ErrInvalidAttempt
	}
	return sha256.Sum256([]byte(value)), nil
}
