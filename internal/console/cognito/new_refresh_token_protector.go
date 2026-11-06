package cognito

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"crypto/subtle"
	"io"
)

func NewRefreshTokenProtector(attempts *AttemptManager, random io.Reader, key []byte) (*RefreshTokenProtector, error) {
	if attempts == nil || random == nil || len(key) != 32 || attempts.keyFingerprint == ([32]byte{}) ||
		attempts.providerBinding == ([32]byte{}) {
		return nil, ErrInvalidRefreshProtectionConfig
	}
	var nonzero byte
	for _, value := range key {
		nonzero |= value
	}
	fingerprint := sha256.Sum256(key)
	if nonzero == 0 || subtle.ConstantTimeCompare(fingerprint[:], attempts.keyFingerprint[:]) == 1 {
		return nil, ErrInvalidRefreshProtectionConfig
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidRefreshProtectionConfig
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrInvalidRefreshProtectionConfig
	}
	return &RefreshTokenProtector{
		aead: aead, random: random, providerBinding: attempts.providerBinding, providerIssuer: attempts.providerIssuer,
	}, nil
}
