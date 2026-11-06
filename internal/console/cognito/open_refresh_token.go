package cognito

func (p *RefreshTokenProtector) Open(binding RefreshTokenBinding, encrypted []byte) (string, error) {
	additional, err := refreshTokenAdditionalData(p.providerBinding, binding)
	minimum := p.aead.NonceSize() + p.aead.Overhead() + 1
	if err != nil || len(encrypted) < minimum || len(encrypted) > minimum+MaxRefreshTokenBytes-1 {
		return "", ErrInvalidRefreshToken
	}
	nonce := encrypted[:p.aead.NonceSize()]
	plaintext, err := p.aead.Open(nil, nonce, encrypted[p.aead.NonceSize():], additional)
	if err != nil {
		return "", ErrRefreshTokenProtection
	}
	token := string(plaintext)
	if !validRefreshToken(token) {
		return "", ErrInvalidRefreshToken
	}
	return token, nil
}
