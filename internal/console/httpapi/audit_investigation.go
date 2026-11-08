package httpapi

import (
	"context"
	"time"
)

// AuditFilter selects audit records; Global or ClusterID is the already-authorized scope.
type AuditFilter struct {
	Global        bool
	ClusterID     string
	ActorID       string
	Action        string
	Outcome       string
	From          time.Time
	Until         time.Time
	AfterSequence int64
	Limit         int
}

// AuditRecord is one sanitized audit event as stored by the Console.
type AuditRecord struct {
	Sequence       int64
	EventID        string
	OperationID    string
	OccurredAt     time.Time
	ActorID        string
	Source         string
	ClusterID      string
	Environment    string
	Action         string
	ResourceKind   string
	ResourceID     string
	Decision       string
	Outcome        string
	Reason         string
	BeforeState    string
	AfterState     string
	PolicyRevision int64
}

// AuditStore reads audit records and appends the audit of each read; the application never changes them.
type AuditStore interface {
	ListAudit(ctx context.Context, filter AuditFilter) ([]AuditRecord, error)
	AppendAudit(ctx context.Context, record AuditRecord) error
}

type auditQuery struct {
	filter AuditFilter
}

type auditEventResponse struct {
	Sequence       int64  `json:"sequence"`
	OccurredAt     string `json:"occurredAt"`
	OperationID    string `json:"operationId"`
	ActorID        string `json:"actorId"`
	Source         string `json:"source"`
	ClusterID      string `json:"clusterId"`
	Environment    string `json:"environment"`
	Action         string `json:"action"`
	ResourceKind   string `json:"resourceKind"`
	ResourceID     string `json:"resourceId"`
	Decision       string `json:"decision"`
	Outcome        string `json:"outcome"`
	Reason         string `json:"reason"`
	BeforeState    string `json:"beforeState"`
	AfterState     string `json:"afterState"`
	PolicyRevision int64  `json:"policyRevision"`
}
