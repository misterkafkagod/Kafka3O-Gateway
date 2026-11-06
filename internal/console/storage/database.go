package storage

import (
	"database/sql"
	"errors"
	"time"
)

type Database struct {
	db   *sql.DB
	path string
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
	ErrInvalidDatabasePath = errors.New("database path must be an absolute regular file")
	ErrInsecureDatabase    = errors.New("database file permissions must restrict access to its owner")
	ErrDatabaseSettings    = errors.New("required SQLite settings are unavailable")
	ErrUnsupportedSchema   = errors.New("database schema version is newer than supported")
	ErrInvalidAuditEvent   = errors.New("invalid audit event")
	ErrInvalidAuditFilter  = errors.New("invalid audit filter")
)
