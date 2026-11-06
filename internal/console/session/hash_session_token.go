package session

import (
	"crypto/sha256"
	"encoding/base64"
)

func hashSessionToken(token string) ([32]byte, error) {
	if len(token) != 43 {
		return [32]byte{}, ErrInvalidSession
	}
	value, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(value) != 32 || base64.RawURLEncoding.EncodeToString(value) != token {
		return [32]byte{}, ErrInvalidSession
	}
	return sha256.Sum256(value), nil
}
