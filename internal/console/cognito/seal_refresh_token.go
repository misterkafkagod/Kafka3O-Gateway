package cognito

import (
	"errors"
	"fmt"
	"io"
)

func (p *RefreshTokenProtector) Seal(binding RefreshTokenBinding, token string) ([]byte, error) {
	additional, err := refreshTokenAdditionalData(p.providerBinding, binding)
	if err != nil || !validRefreshToken(token) {
		return nil, ErrInvalidRefreshToken
	}
	nonce := make([]byte, p.aead.NonceSize())
	p.mu.Lock()
	_, err = io.ReadFull(p.random, nonce)
	p.mu.Unlock()
	if err != nil {
		return nil, errors.Join(ErrRefreshTokenProtection, fmt.Errorf("generate refresh-token encryption nonce: %w", err))
	}
	return p.aead.Seal(nonce, nonce, []byte(token), additional), nil
}
