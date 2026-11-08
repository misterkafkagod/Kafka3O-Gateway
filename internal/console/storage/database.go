package storage

import (
	"database/sql"
	"errors"
	"os"
	"sync"
	"time"
)

type Database struct {
	db   *sql.DB
	path string
	lock *os.File
	now  func() time.Time

	closeOnce sync.Once
	closeErr  error
}

type AuditSource string

const (
	AuditSourceAnonymous AuditSource = "anonymous"
	AuditSourceLocal     AuditSource = "local"
	AuditSourceCognito   AuditSource = "cognito"
	AuditSourceService   AuditSource = "service"
)

type AuditOutcome string

const (
	AuditAttempted          AuditOutcome = "attempted"
	AuditDenied             AuditOutcome = "denied"
	AuditValidationRejected AuditOutcome = "validation_rejected"
	AuditAccepted           AuditOutcome = "accepted"
	AuditConfirmed          AuditOutcome = "confirmed"
	AuditFailedBeforeSend   AuditOutcome = "failed_before_send"
	AuditUnknown            AuditOutcome = "unknown"
	AuditSucceeded          AuditOutcome = "succeeded"
	AuditFailed             AuditOutcome = "failed"
)

type AuditEvent struct {
	Sequence       int64
	EventID        string
	OperationID    string
	OccurredAt     time.Time
	ActorID        string
	Source         AuditSource
	ClusterID      string
	Environment    string
	Action         string
	ResourceKind   string
	ResourceID     string
	Decision       string
	Outcome        AuditOutcome
	Reason         string
	PolicyRevision int64
	// BeforeState and AfterState hold only a role or environment token, never free text.
	BeforeState string
	AfterState  string
}

// AuditFilter limits storage results; callers must separately authorize the requested scope.
type AuditFilter struct {
	Global        bool
	ClusterID     string
	ActorID       string
	Action        string
	Outcome       AuditOutcome
	From          time.Time
	Until         time.Time
	AfterSequence int64
	Limit         int
}

var (
	ErrInvalidDatabasePath          = errors.New("database path must be an absolute regular file")
	ErrInsecureDatabase             = errors.New("database file permissions must restrict access to its owner")
	ErrDatabaseSettings             = errors.New("required SQLite settings are unavailable")
	ErrDatabaseInUse                = errors.New("Console database is already in use")
	ErrInvalidDatabaseLock          = errors.New("Console database lock must be an owner-only regular file")
	ErrUnsupportedLock              = errors.New("Console database locking is unsupported on this platform")
	ErrBootstrapRequired            = errors.New("initial local administrator has not been bootstrapped")
	ErrNoLocalGlobalAdmin           = errors.New("no enabled local global administrator is available; recovery is required")
	ErrUnsupportedSchema            = errors.New("database schema version is newer than supported")
	ErrInvalidAuditEvent            = errors.New("invalid audit event")
	ErrInvalidAuditFilter           = errors.New("invalid audit filter")
	ErrCognitoCleanupConfig         = errors.New("invalid Cognito credential cleanup request")
	ErrCognitoCleanupUnknown        = errors.New("Cognito credential cleanup outcome is unknown")
	ErrBrowserBindingCleanupConfig  = errors.New("invalid browser binding cleanup request")
	ErrBrowserBindingCleanupUnknown = errors.New("browser binding cleanup outcome is unknown")
)
