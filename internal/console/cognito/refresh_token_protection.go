package cognito

import (
	"crypto/cipher"
	"errors"
	"io"
	"sync"
)

const MaxRefreshTokenBytes = 8192

type RefreshTokenBinding struct {
	SessionID  string
	IdentityID string
	Generation int64
}

type RefreshTokenProtector struct {
	aead            cipher.AEAD
	random          io.Reader
	providerBinding [32]byte
	mu              sync.Mutex
}

var (
	ErrInvalidRefreshProtectionConfig = errors.New("invalid Cognito refresh-token protection configuration")
	ErrInvalidRefreshToken            = errors.New("invalid Cognito refresh token or binding")
	ErrRefreshTokenProtection         = errors.New("Cognito refresh-token protection failed")
)
