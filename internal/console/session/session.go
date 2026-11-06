package session

import (
	"context"
	"errors"
	"io"
	"time"
)

const (
	DefaultIdleLifetime     = 30 * time.Minute
	DefaultAbsoluteLifetime = 8 * time.Hour
	MaxAbsoluteLifetime     = 24 * time.Hour
)

type Principal struct {
	SessionID          string
	AccountID          string
	Username           string
	MustChangePassword bool
	CreatedAt          time.Time
	IdleExpiresAt      time.Time
	AbsoluteExpiresAt  time.Time
	CSRFHash           [32]byte `json:"-"`
}

type Credentials struct {
	Principal Principal
	Token     string `json:"-"`
	CSRFToken string `json:"-"`
}

type Record struct {
	SessionID         string
	AccountID         string
	CredentialVersion int64
	TokenHash         [32]byte `json:"-"`
	CSRFHash          [32]byte `json:"-"`
	CreatedAt         time.Time
	IdleExpiresAt     time.Time
	AbsoluteExpiresAt time.Time
	EventID           string
}

type Store interface {
	CreateLocalSession(ctx context.Context, record Record) (Principal, error)
	UseSession(ctx context.Context, tokenHash [32]byte, now, newIdleExpiry time.Time) (Principal, error)
	RevokeSession(ctx context.Context, tokenHash [32]byte, eventID string, now time.Time) error
}

type Manager struct {
	store            Store
	random           io.Reader
	now              func() time.Time
	idleLifetime     time.Duration
	absoluteLifetime time.Duration
}

type AccessPurpose string

const (
	AccessNormal         AccessPurpose = "normal"
	AccessPasswordChange AccessPurpose = "password_change"
	AccessLogout         AccessPurpose = "logout"
	AccessSessionInfo    AccessPurpose = "session_info"
)

var (
	ErrInvalidSessionConfig   = errors.New("invalid session configuration")
	ErrInvalidSession         = errors.New("session is unavailable or expired")
	ErrPasswordChangeRequired = errors.New("password change is required")
	ErrInvalidAccessPurpose   = errors.New("invalid session access purpose")
	ErrInvalidCSRF            = errors.New("request origin or CSRF token is invalid")
)

func NewManager(store Store, random io.Reader, now func() time.Time, idleLifetime, absoluteLifetime time.Duration) (*Manager, error) {
	if store == nil || random == nil || now == nil || idleLifetime <= 0 ||
		absoluteLifetime < idleLifetime || absoluteLifetime > MaxAbsoluteLifetime {
		return nil, ErrInvalidSessionConfig
	}
	return &Manager{store: store, random: random, now: now, idleLifetime: idleLifetime, absoluteLifetime: absoluteLifetime}, nil
}
