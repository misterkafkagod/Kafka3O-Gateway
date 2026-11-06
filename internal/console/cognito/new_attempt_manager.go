package cognito

import (
	"crypto/aes"
	"crypto/cipher"
	"io"
	"time"
)

func NewAttemptManager(store AttemptStore, random io.Reader, now func() time.Time, key []byte) (*AttemptManager, error) {
	if store == nil || random == nil || now == nil || len(key) != 32 {
		return nil, ErrInvalidAttemptConfig
	}
	var nonzero byte
	for _, value := range key {
		nonzero |= value
	}
	if nonzero == 0 {
		return nil, ErrInvalidAttemptConfig
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidAttemptConfig
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrInvalidAttemptConfig
	}
	return &AttemptManager{store: store, random: random, now: now, aead: aead}, nil
}
