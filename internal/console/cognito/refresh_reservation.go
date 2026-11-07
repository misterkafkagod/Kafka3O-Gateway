package cognito

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type RefreshStore interface {
	ReserveCognitoRefresh(context.Context, RefreshReservationRequest) (RefreshReservation, error)
	RecordCognitoRefreshFailure(context.Context, RefreshFailure) error
}

type BoundRefreshStore interface {
	RefreshStore
	ReserveBoundCognitoRefresh(context.Context, RefreshReservationRequest, [32]byte) (RefreshReservation, error)
	ValidateBoundCognitoRefreshDispatch(context.Context, BoundRefreshDispatchRequest) error
	RecordBoundCognitoRefreshFailure(context.Context, RefreshFailure, [32]byte) error
}

type RefreshReservationRequest struct {
	OperationID string
	TokenHash   [32]byte               `json:"-"`
	Protector   *RefreshTokenProtector `json:"-"`
	OccurredAt  time.Time
}

type RefreshReservation struct {
	OperationID              string
	SessionID                string
	IdentityID               string
	Generation               int64
	PreviousIdentity         VerifiedIdentity `json:"-"`
	PreviousClaimsFreshUntil time.Time
	OriginalNonceHash        [32]byte `json:"-"`
	EncryptedRefreshToken    []byte   `json:"-"`
	IdleExpiresAt            time.Time
	AbsoluteExpiresAt        time.Time
}

type BoundRefreshDispatchRequest struct {
	TokenHash          [32]byte           `json:"-"`
	BrowserBindingHash [32]byte           `json:"-"`
	Reservation        RefreshReservation `json:"-"`
	OccurredAt         time.Time
}

type RefreshFailureStatus string

const (
	RefreshFailedBeforeSend RefreshFailureStatus = "failed_before_send"
	RefreshOutcomeUnknown   RefreshFailureStatus = "unknown"
)

type RefreshFailure struct {
	OperationID string
	SessionID   string
	Generation  int64
	OccurredAt  time.Time
	Status      RefreshFailureStatus
}

type RefreshExchangeResult struct {
	OperationID string
	proof       *verifiedRefreshProof
}

type verifiedRefreshProof struct {
	operationID              string
	sessionID                string
	identityID               string
	previousGeneration       int64
	previousIdentity         VerifiedIdentity
	previousClaimsFreshUntil time.Time
	previousEncryptedHash    [32]byte
	identity                 VerifiedIdentity
	nonceHash                [32]byte
	encryptedRefreshToken    []byte
	providerBinding          [32]byte
	browserBindingHash       [32]byte
	verifiedAt               time.Time
	claimsFreshUntil         time.Time
	sessionDeadline          time.Time
	completionAttempted      atomic.Bool
}

type VerifiedRefresh struct {
	proof *verifiedRefreshProof
}

type RefreshCompletionDetails struct {
	OperationID              string
	SessionID                string
	IdentityID               string
	PreviousGeneration       int64
	PreviousIdentity         VerifiedIdentity `json:"-"`
	PreviousClaimsFreshUntil time.Time
	PreviousEncryptedHash    [32]byte         `json:"-"`
	Identity                 VerifiedIdentity `json:"-"`
	OriginalNonceHash        [32]byte         `json:"-"`
	EncryptedRefreshToken    []byte           `json:"-"`
	ClaimsFreshUntil         time.Time
	ValidUntil               time.Time
}

type RefreshCompletionRequest struct {
	Refresh    VerifiedRefresh        `json:"-"`
	Protector  *RefreshTokenProtector `json:"-"`
	OccurredAt time.Time
}

var (
	ErrRefreshUnavailable = errors.New("Cognito session refresh is unavailable or already reserved")
	ErrRefreshUnknown     = errors.New("Cognito session refresh outcome is unknown")
	ErrRefreshAudit       = errors.New("Cognito session refresh audit failed")
	ErrRefreshState       = errors.New("Cognito session refresh state is invalid")
)
