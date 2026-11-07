package session

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

const (
	DefaultIdleLifetime     = 30 * time.Minute
	DefaultAbsoluteLifetime = 8 * time.Hour
	MaxAbsoluteLifetime     = 24 * time.Hour
	maxCognitoRefreshes     = 32
	cognitoRefreshTimeout   = 15 * time.Second
)

type Principal struct {
	SessionID          string
	AccountID          string
	Username           string
	Source             AuthenticationSource
	MustChangePassword bool
	CreatedAt          time.Time
	IdleExpiresAt      time.Time
	AbsoluteExpiresAt  time.Time
	CSRFHash           [32]byte `json:"-"`
}

type AuthenticationSource string

const (
	SourceLocal   AuthenticationSource = "local"
	SourceCognito AuthenticationSource = "cognito"
)

type Credentials struct {
	Principal Principal
	Token     string `json:"-"`
	CSRFToken string `json:"-"`
}

type Record struct {
	SessionID          string
	AccountID          string
	CredentialVersion  int64
	TokenHash          [32]byte `json:"-"`
	CSRFHash           [32]byte `json:"-"`
	BrowserBindingHash [32]byte `json:"-"`
	CreatedAt          time.Time
	IdleExpiresAt      time.Time
	AbsoluteExpiresAt  time.Time
	EventID            string
}

// Store returns ErrInvalidSession directly only when invalidity is certain.
// Wrapped or joined errors must be treated as operational failures.
type Store interface {
	CreateLocalSession(ctx context.Context, record Record) (Principal, error)
	UseSession(ctx context.Context, tokenHash [32]byte, now func() time.Time, idleLifetime time.Duration, protector *cognito.RefreshTokenProtector) (Principal, error)
	RevokeSession(ctx context.Context, tokenHash [32]byte, eventID string, now time.Time) error
}

type CognitoRecord struct {
	Session          Record
	CandidateAccount string
	Exchange         cognito.VerifiedCodeExchange   `json:"-"`
	Protector        *cognito.RefreshTokenProtector `json:"-"`
}

type CognitoSessionStore interface {
	CreateCognitoSession(context.Context, CognitoRecord) (Principal, error)
	FindCognitoSessionForLogout(context.Context, [32]byte) (Principal, error)
}

type BoundCognitoLogoutStore interface {
	FindBoundCognitoSessionForLogout(context.Context, [32]byte, [32]byte) (Principal, error)
}

type BoundSessionRevokeStore interface {
	RevokeBoundSession(context.Context, [32]byte, [32]byte, string, time.Time) error
}

// CognitoRefreshStore follows the same exact ErrInvalidSession contract as Store.
type CognitoRefreshStore interface {
	cognito.RefreshStore
	InspectCognitoRefresh(context.Context, [32]byte, time.Time) (CognitoRefreshState, error)
	CompleteCognitoRefresh(context.Context, cognito.RefreshCompletionRequest) error
}

type BoundSessionStore interface {
	UseBoundSession(context.Context, [32]byte, [32]byte, func() time.Time, time.Duration, *cognito.RefreshTokenProtector) (Principal, error)
}

type BoundCognitoRefreshStore interface {
	cognito.BoundRefreshStore
	InspectBoundCognitoRefresh(context.Context, [32]byte, [32]byte, time.Time) (CognitoRefreshState, error)
	CompleteBoundCognitoRefresh(context.Context, cognito.RefreshCompletionRequest, [32]byte) error
}

// SessionGrantStore revalidates the session and returns only grants for the
// requested scope. ErrInvalidSession has the same exact contract as Store.
type SessionGrantStore interface {
	ResolveSessionGrants(context.Context, [32]byte, string, time.Time, *cognito.RefreshTokenProtector) ([]authorization.Grant, error)
}

type BoundSessionGrantStore interface {
	ResolveBoundSessionGrants(context.Context, [32]byte, [32]byte, string, time.Time, *cognito.RefreshTokenProtector) ([]authorization.Grant, error)
}

type CognitoRefreshState uint8

const (
	CognitoRefreshReady CognitoRefreshState = iota + 1
	CognitoRefreshFresh
	CognitoRefreshBlocked
)

type Manager struct {
	store            Store
	random           io.Reader
	now              func() time.Time
	idleLifetime     time.Duration
	absoluteLifetime time.Duration
	cognitoProtector *cognito.RefreshTokenProtector
	cognitoRefresh   *cognitoRefreshCoordinator
}

type cognitoRefreshCoordinator struct {
	store     CognitoRefreshStore
	exchanger *cognito.CodeExchanger
	mu        sync.Mutex
	active    map[cognitoRefreshKey]*cognitoRefreshCall
}

type cognitoRefreshKey struct {
	tokenHash   [32]byte
	browserHash [32]byte
}

type cognitoRefreshCall struct {
	done chan struct{}
	err  error
}

type AccessPurpose string

const (
	AccessNormal         AccessPurpose = "normal"
	AccessPasswordChange AccessPurpose = "password_change"
	AccessLogout         AccessPurpose = "logout"
	AccessSessionInfo    AccessPurpose = "session_info"
)

var (
	ErrInvalidSessionConfig      = errors.New("invalid session configuration")
	ErrInvalidSession            = errors.New("session is unavailable or expired")
	ErrPasswordChangeRequired    = errors.New("password change is required")
	ErrInvalidAccessPurpose      = errors.New("invalid session access purpose")
	ErrInvalidCSRF               = errors.New("request origin or CSRF token is invalid")
	ErrCognitoSessionUnavailable = errors.New("Cognito session is unavailable or already used")
	ErrCognitoSessionUnknown     = errors.New("Cognito session outcome is unknown")
	ErrCognitoSessionAudit       = errors.New("Cognito session audit failed")
	ErrCognitoRefreshUnavailable = errors.New("Cognito session refresh is unavailable")
	// Only the exact conflict sentinel confirms a durably audited denial.
	ErrBrowserSessionConflict = errors.New("browser already has an active session")
	ErrBrowserSessionUnknown  = errors.New("browser session outcome is unavailable")
	ErrBrowserSessionAudit    = errors.New("browser session audit failed")
)

func NewManager(store Store, random io.Reader, now func() time.Time, idleLifetime, absoluteLifetime time.Duration) (*Manager, error) {
	if store == nil || random == nil || now == nil || idleLifetime <= 0 ||
		absoluteLifetime < idleLifetime || absoluteLifetime > MaxAbsoluteLifetime {
		return nil, ErrInvalidSessionConfig
	}
	return &Manager{store: store, random: random, now: now, idleLifetime: idleLifetime, absoluteLifetime: absoluteLifetime}, nil
}
