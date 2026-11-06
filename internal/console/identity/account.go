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
	ReserveBootstrapAttempt(ctx context.Context, attempt BootstrapAttempt) error
	FailBootstrapAttempt(ctx context.Context, failure BootstrapFailure) error
	CreateInitialAdmin(ctx context.Context, account LocalAccount, eventID, operationID string, occurredAt time.Time) error
}

type BootstrapAttempt struct {
	EventID    string
	OccurredAt time.Time
}

type BootstrapFailure struct {
	EventID     string
	OperationID string
	OccurredAt  time.Time
	Denied      bool
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

type LocalAdminActor struct {
	AccountID string
	SessionID string
}

type LocalAccountStateChange struct {
	Actor             LocalAdminActor
	TargetID          string
	Enabled           bool
	OperationID       string
	EventID           string
	RevocationEventID string
	OccurredAt        time.Time
}

type LocalAccountStateStore interface {
	SetLocalAccountEnabled(ctx context.Context, change LocalAccountStateChange) error
}

type LocalPasswordResetAttempt struct {
	Actor       LocalAdminActor
	TargetID    string
	OperationID string
	OccurredAt  time.Time
}

type LocalPasswordResetFailure struct {
	Actor       LocalAdminActor
	TargetID    string
	OperationID string
	EventID     string
	OccurredAt  time.Time
	Rejected    bool
}

type LocalPasswordResetCompletion struct {
	Actor             LocalAdminActor
	TargetID          string
	OperationID       string
	EventID           string
	RevocationEventID string
	NewHash           string `json:"-"`
	OccurredAt        time.Time
}

type LocalPasswordResetStore interface {
	ReserveLocalPasswordReset(ctx context.Context, attempt LocalPasswordResetAttempt) error
	FailLocalPasswordReset(ctx context.Context, failure LocalPasswordResetFailure) error
	CompleteLocalPasswordReset(ctx context.Context, completion LocalPasswordResetCompletion) error
}

type PasswordResetResult struct {
	OperationID string
}

type LocalAccountCreationAttempt struct {
	Actor       LocalAdminActor
	AccountID   string
	OperationID string
	OccurredAt  time.Time
}

type LocalAccountCreationFailure struct {
	Actor       LocalAdminActor
	AccountID   string
	OperationID string
	EventID     string
	OccurredAt  time.Time
	Rejected    bool
}

type LocalAccountCreationCompletion struct {
	Actor       LocalAdminActor
	Account     LocalAccount
	OperationID string
	EventID     string
	OccurredAt  time.Time
}

type LocalAccountCreationStore interface {
	ReserveLocalAccountCreation(ctx context.Context, attempt LocalAccountCreationAttempt) error
	FailLocalAccountCreation(ctx context.Context, failure LocalAccountCreationFailure) error
	CompleteLocalAccountCreation(ctx context.Context, completion LocalAccountCreationCompletion) error
}

type LocalAccountCreationResult struct {
	OperationID string
	Account     LocalAccount
}

var (
	ErrInvalidUsername         = errors.New("invalid local username")
	ErrInvalidBootstrapConfig  = errors.New("a strong bootstrap secret and dependencies are required")
	ErrInvalidBootstrapSecret  = errors.New("bootstrap authentication failed")
	ErrBootstrapThrottled      = errors.New("bootstrap temporarily throttled")
	ErrAlreadyBootstrapped     = errors.New("local administrator has already been bootstrapped")
	ErrAccountNotFound         = errors.New("local account not found")
	ErrInvalidLocalAccount     = errors.New("invalid local account")
	ErrAuthenticationFailed    = errors.New("authentication failed")
	ErrAuthenticationThrottled = errors.New("authentication temporarily throttled")
	ErrAuthenticationConfig    = errors.New("local authentication dependencies are required")
	ErrInvalidClientAddress    = errors.New("a valid client address is required")
	ErrPasswordUnchanged       = errors.New("the replacement password must differ from the current password")
	ErrInvalidAdminSession     = errors.New("administrator session is unavailable or expired")
	ErrGlobalAdminRequired     = errors.New("global administrator access is required")
	ErrAdminPasswordChange     = errors.New("administrator must change the temporary password")
	ErrLastLocalGlobalAdmin    = errors.New("the last enabled local global administrator cannot be disabled")
	ErrAccountAdminConfig      = errors.New("account administration dependencies are required")
	ErrPasswordResetThrottled  = errors.New("password reset temporarily throttled")
	ErrInvalidPasswordReset    = errors.New("password reset attempt is unavailable or already used")
	ErrPasswordResetExpired    = errors.New("password reset attempt has expired")
	ErrLocalAccountChanged     = errors.New("local account changed during password reset")
	ErrPasswordResetUnknown    = errors.New("password reset outcome is unknown")
	ErrPasswordResetAudit      = errors.New("password reset failure could not be audited")
	ErrUsernameTaken           = errors.New("local username is already in use")
	ErrInvalidAccountCreation  = errors.New("local account creation attempt is unavailable or already used")
	ErrAccountCreationExpired  = errors.New("local account creation attempt has expired")
	ErrAccountCreationUnknown  = errors.New("local account creation outcome is unknown")
	ErrAccountCreationAudit    = errors.New("local account creation failure could not be audited")
)
