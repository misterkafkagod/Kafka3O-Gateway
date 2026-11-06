package cognito

import (
	"crypto/aes"
	"crypto/cipher"
	"io"
	"time"
)

func NewAttemptManager(store AttemptStore, random io.Reader, now func() time.Time, key []byte,
	provider ProviderConfig, callbackURL string) (*AttemptManager, error) {
	if store == nil || random == nil || now == nil || len(key) != 32 {
		return nil, ErrInvalidAttemptConfig
	}
	binding, err := authorizationProviderBinding(provider, callbackURL)
	if err != nil {
		return nil, err
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
	return &AttemptManager{store: store, random: random, now: now, aead: aead, providerBinding: binding}, nil
}
