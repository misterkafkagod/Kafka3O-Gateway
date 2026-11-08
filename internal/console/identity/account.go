package identity

import (
	"context"
	"errors"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
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
	AccountID          string
	SessionID          string
	BrowserBindingHash [32]byte `json:"-"`
}

type AdminSessionRevocation struct {
	Actor       LocalAdminActor
	SessionID   string
	OperationID string
	EventID     string
	OccurredAt  time.Time
}

type AdminSessionRevocationResult struct {
	OperationID string
}

type AdminSessionRevocationStore interface {
	RevokeAdminSession(ctx context.Context, revocation AdminSessionRevocation) error
}

const (
	DefaultAdminSessionPageSize = 50
	MaxAdminSessionPageSize     = 100
)

type AdminSessionSummary struct {
	ID                string
	AccountID         string
	Username          string
	Source            string
	CreatedAt         time.Time
	IdleExpiresAt     time.Time
	AbsoluteExpiresAt time.Time
	Current           bool
}

type AdminSessionListRequest struct {
	Actor       LocalAdminActor
	AfterID     string
	Limit       int
	OperationID string
	OccurredAt  time.Time
}

type AdminSessionListResult struct {
	OperationID string
	Sessions    []AdminSessionSummary
	NextCursor  string
}

type AdminSessionListStore interface {
	ListAdminSessions(ctx context.Context, request AdminSessionListRequest) (AdminSessionListResult, error)
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

type LocalAccountStateResult struct {
	OperationID string
}

const (
	DefaultLocalAccountPageSize = 50
	MaxLocalAccountPageSize     = 100
)

type LocalAccountSummary struct {
	ID                 string
	Username           string
	Enabled            bool
	MustChangePassword bool
}

type LocalAccountListRequest struct {
	Actor       LocalAdminActor
	AfterID     string
	Limit       int
	OperationID string
	OccurredAt  time.Time
}

type LocalAccountListResult struct {
	OperationID string
	Accounts    []LocalAccountSummary
	NextCursor  string
}

type LocalAccountListStore interface {
	ListLocalAccounts(ctx context.Context, request LocalAccountListRequest) (LocalAccountListResult, error)
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
	ErrInvalidUsername           = errors.New("invalid local username")
	ErrInvalidBootstrapConfig    = errors.New("a strong bootstrap secret and dependencies are required")
	ErrInvalidBootstrapSecret    = errors.New("bootstrap authentication failed")
	ErrBootstrapThrottled        = errors.New("bootstrap temporarily throttled")
	ErrAlreadyBootstrapped       = errors.New("local administrator has already been bootstrapped")
	ErrAccountNotFound           = errors.New("local account not found")
	ErrInvalidLocalAccount       = errors.New("invalid local account")
	ErrAuthenticationFailed      = errors.New("authentication failed")
	ErrAuthenticationThrottled   = errors.New("authentication temporarily throttled")
	ErrAuthenticationConfig      = errors.New("local authentication dependencies are required")
	ErrInvalidClientAddress      = errors.New("a valid client address is required")
	ErrPasswordUnchanged         = errors.New("the replacement password must differ from the current password")
	ErrInvalidAdminSession       = errors.New("administrator session is unavailable or expired")
	ErrGlobalAdminRequired       = errors.New("global administrator access is required")
	ErrAdminPasswordChange       = errors.New("administrator must change the temporary password")
	ErrInvalidSessionID          = errors.New("invalid session identifier")
	ErrAdminSessionUnavailable   = errors.New("target session is unavailable or expired")
	ErrAdminSessionRevokeAudit   = errors.New("session revocation could not be audited")
	ErrAdminSessionRevokeUnknown = errors.New("session revocation outcome is unknown")
	ErrInvalidAdminSessionList   = errors.New("invalid session listing request")
	ErrInvalidAdminSessionCursor = errors.New("invalid session listing cursor")
	ErrAdminSessionListAudit     = errors.New("session listing could not be audited")
	ErrLastLocalGlobalAdmin      = errors.New("the last enabled local global administrator cannot be disabled")
	ErrAccountAdminConfig        = errors.New("account administration dependencies are required")
	ErrAccountStateUnknown       = errors.New("local account state change outcome is unknown")
	ErrAccountStateAudit         = errors.New("local account state change could not be audited")
	ErrInvalidAccountList        = errors.New("invalid local account listing request")
	ErrInvalidAccountCursor      = errors.New("local account listing cursor is invalid")
	ErrAccountListAudit          = errors.New("local account listing could not be audited")
	ErrPasswordResetThrottled    = errors.New("password reset temporarily throttled")
	ErrInvalidPasswordReset      = errors.New("password reset attempt is unavailable or already used")
	ErrPasswordResetExpired      = errors.New("password reset attempt has expired")
	ErrLocalAccountChanged       = errors.New("local account changed during password reset")
	ErrPasswordResetUnknown      = errors.New("password reset outcome is unknown")
	ErrPasswordResetAudit        = errors.New("password reset failure could not be audited")
	ErrUsernameTaken             = errors.New("local username is already in use")
	ErrInvalidAccountCreation    = errors.New("local account creation attempt is unavailable or already used")
	ErrAccountCreationExpired    = errors.New("local account creation attempt has expired")
	ErrAccountCreationUnknown    = errors.New("local account creation outcome is unknown")
	ErrAccountCreationAudit      = errors.New("local account creation failure could not be audited")
	ErrAccessAdminConfig         = errors.New("access administration dependencies are required")
	ErrInvalidAccessChange       = errors.New("invalid access change")
	ErrAccessTargetNotFound      = errors.New("access change target identity not found")
	ErrSelfAccessChange          = errors.New("administrators cannot change their own access")
	ErrPolicyConflict            = errors.New("access policy changed since it was read")
	ErrAccessChangeAudit         = errors.New("access change could not be audited")
	ErrAccessChangeUnknown       = errors.New("access change outcome is unknown")
	ErrInvalidAccessList         = errors.New("invalid access listing request")
	ErrAccessListAudit           = errors.New("access listing could not be audited")
	ErrInvalidClusterID          = errors.New("invalid cluster identifier")
	ErrInvalidClusterRegistry    = errors.New("invalid registered cluster configuration")
	ErrClusterRetarget           = errors.New("a registered cluster cannot be pointed at a different Gateway")
	ErrUnknownCluster            = errors.New("cluster is not an active registered cluster")
	ErrChangeReasonRequired      = errors.New("a valid change reason is required")
)

// RoleGrantChange sets (or, with an empty Role, removes) one direct grant; an empty ClusterID is global.
type RoleGrantChange struct {
	Actor        LocalAdminActor
	TargetID     string
	ClusterID    string
	ExpectedRole authorization.Role
	Role         authorization.Role
	OperationID  string
	EventID      string
	OccurredAt   time.Time
}

type AccessChangeResult struct {
	OperationID    string
	PolicyRevision int64
}

type RoleGrantStore interface {
	ChangeRoleGrant(ctx context.Context, change RoleGrantChange) (int64, error)
}

// GroupMappingChange sets (or, with an empty Role, removes) the role an exact issuer and
// case-sensitive verified Cognito group receive; an empty ClusterID is global.
type GroupMappingChange struct {
	Actor        LocalAdminActor
	Issuer       string
	Group        string
	ClusterID    string
	ExpectedRole authorization.Role
	Role         authorization.Role
	OperationID  string
	EventID      string
	OccurredAt   time.Time
}

type GroupMappingStore interface {
	ChangeGroupMapping(ctx context.Context, change GroupMappingChange) (int64, error)
}

const (
	MaxAccessPageSize    = 100
	MaxListedGroupGrants = 1000
)

// AccessIdentity is one local or Cognito identity with its direct grants; it never carries credentials or email.
type AccessIdentity struct {
	ID      string
	Source  string
	Label   string
	Enabled bool
	Grants  []authorization.Grant
}

type GroupGrant struct {
	Issuer string
	Group  string
	Grant  authorization.Grant
}

type AccessListRequest struct {
	Actor       LocalAdminActor
	AfterID     string
	Limit       int
	OperationID string
	OccurredAt  time.Time
}

type AccessListResult struct {
	OperationID          string
	Identities           []AccessIdentity
	NextCursor           string
	GroupGrants          []GroupGrant
	GroupGrantsTruncated bool
	PolicyRevision       int64
}

type AccessListStore interface {
	ListAccessBindings(ctx context.Context, request AccessListRequest) (AccessListResult, error)
}

// RegisteredCluster is one operator-configured cluster; its environment is held only by the Console store.
type RegisteredCluster struct {
	ID         string
	Name       string
	GatewayURL string
}

// ClusterEnvironmentChange classifies one active registered cluster; Environment is never unclassified.
type ClusterEnvironmentChange struct {
	Actor               LocalAdminActor
	ClusterID           string
	ExpectedEnvironment authorization.Environment
	Environment         authorization.Environment
	Reason              string
	OperationID         string
	EventID             string
	OccurredAt          time.Time
}

type ClusterEnvironmentStore interface {
	ChangeClusterEnvironment(ctx context.Context, change ClusterEnvironmentChange) (int64, error)
}
