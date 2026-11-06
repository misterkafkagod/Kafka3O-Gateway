package identity

import (
	"context"
	"errors"
	"time"
)

type LocalAccount struct {
	ID                 string
	Username           string
	PasswordHash       string `json:"-"`
	Enabled            bool
	MustChangePassword bool
	CredentialVersion  int64 `json:"-"`
}

type InitialAdminStore interface {
	CreateInitialAdmin(ctx context.Context, account LocalAccount, eventID string, occurredAt time.Time) error
}

type FindLocalAccount interface {
	FindLocalAccount(ctx context.Context, username string) (LocalAccount, error)
}

type LocalAuthenticationAttempt struct {
	PrincipalKey [32]byte
	ClientKey    [32]byte
	EventID      string
	OccurredAt   time.Time
}

type LocalAuthenticationResult struct {
	AccountID         string
	PasswordHash      string `json:"-"`
	CredentialVersion int64  `json:"-"`
	EventID           string
	OperationID       string
	OccurredAt        time.Time
}

type LocalAuthenticationStore interface {
	FindLocalAccount
	ReserveLocalAuthentication(ctx context.Context, attempt LocalAuthenticationAttempt) error
	FinishLocalAuthentication(ctx context.Context, result LocalAuthenticationResult) (bool, error)
}

type LocalPasswordUpdate struct {
	AccountID         string
	PreviousHash      string `json:"-"`
	NewHash           string `json:"-"`
	EventID           string
	RevocationEventID string
	OccurredAt        time.Time
}

type LocalPasswordStore interface {
	FindLocalAccount
	UpdateLocalPassword(ctx context.Context, update LocalPasswordUpdate) error
}

var (
	ErrInvalidUsername         = errors.New("invalid local username")
	ErrInvalidBootstrapConfig  = errors.New("a strong bootstrap secret and dependencies are required")
	ErrInvalidBootstrapSecret  = errors.New("bootstrap authentication failed")
	ErrAlreadyBootstrapped     = errors.New("local administrator has already been bootstrapped")
	ErrAccountNotFound         = errors.New("local account not found")
	ErrInvalidLocalAccount     = errors.New("invalid local account")
	ErrAuthenticationFailed    = errors.New("authentication failed")
	ErrAuthenticationThrottled = errors.New("authentication temporarily throttled")
	ErrAuthenticationConfig    = errors.New("local authentication dependencies are required")
	ErrInvalidClientAddress    = errors.New("a valid client address is required")
	ErrPasswordUnchanged       = errors.New("the replacement password must differ from the current password")
)
