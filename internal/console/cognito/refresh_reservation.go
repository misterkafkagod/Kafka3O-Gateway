package cognito

import (
	"errors"
	"time"
)

type RefreshReservationRequest struct {
	OperationID string
	TokenHash   [32]byte               `json:"-"`
	Protector   *RefreshTokenProtector `json:"-"`
	OccurredAt  time.Time
}

type RefreshReservation struct {
	OperationID           string
	SessionID             string
	IdentityID            string
	Generation            int64
	PreviousIdentity      VerifiedIdentity `json:"-"`
	OriginalNonceHash     [32]byte         `json:"-"`
	EncryptedRefreshToken []byte           `json:"-"`
	IdleExpiresAt         time.Time
	AbsoluteExpiresAt     time.Time
}

var (
	ErrRefreshUnavailable = errors.New("Cognito session refresh is unavailable or already reserved")
	ErrRefreshUnknown     = errors.New("Cognito session refresh outcome is unknown")
	ErrRefreshAudit       = errors.New("Cognito session refresh audit failed")
	ErrRefreshState       = errors.New("Cognito session refresh state is invalid")
)
