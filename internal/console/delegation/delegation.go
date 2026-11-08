// Package delegation mints and consumes the single-use, request-bound Console delegations that let a
// secured Gateway ask for a current authorization decision (see the gateway-service-trust discovery).
package delegation

import (
	"context"
	"errors"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

// Lifetime bounds queueing between minting and the Gateway's consume call; it never permits a retry.
const Lifetime = 30 * time.Second

// Record is a stored delegation; only the digest of its handle is kept.
type Record struct {
	HandleHash [32]byte
	SessionID  string
	ClusterID  string
	Action     authorization.Action
	Resource   string
	Binding    [32]byte
	Reason     string
	CreatedAt  time.Time
	ExpiresAt  time.Time
}

// Consumed is a delegation removed by consume, with the session it was minted for.
type Consumed struct {
	Record
	TokenHash   [32]byte
	BrowserHash [32]byte
	AccountID   string
	Source      string
}

// ConsumeRequest is what a Gateway presents: its verified cluster identity and the request it actually received.
type ConsumeRequest struct {
	Handle          string
	CallerClusterID string
	Action          authorization.Action
	Binding         [32]byte
}

type Decision struct {
	OperationID string
	Allowed     bool
	ActorID     string
	Source      string
	Denial      authorization.Denial
}

// DecisionRecord is the audited result of one consume call.
type DecisionRecord struct {
	OperationID string
	OccurredAt  time.Time
	ActorID     string
	Source      string
	ClusterID   string
	Environment authorization.Environment
	Action      authorization.Action
	Resource    string
	Allowed     bool
	Reason      string
	Denial      string
}

type Store interface {
	CreateGatewayDelegation(ctx context.Context, record Record) error
	ConsumeGatewayDelegation(ctx context.Context, handleHash [32]byte, at time.Time) (Consumed, error)
	RecordGatewayDecision(ctx context.Context, record DecisionRecord) error
}

type SessionGrants interface {
	ResolveSessionGrants(ctx context.Context, tokenHash [32]byte, clusterID string, requested time.Time, protector *cognito.RefreshTokenProtector) ([]authorization.Grant, error)
	ResolveBoundSessionGrants(ctx context.Context, tokenHash, browserHash [32]byte, clusterID string, requested time.Time, protector *cognito.RefreshTokenProtector) ([]authorization.Grant, error)
}

// ProtectorSource returns the current Cognito refresh-token protector, or nil while Cognito is unavailable.
type ProtectorSource func() *cognito.RefreshTokenProtector

type Environments interface {
	ClusterEnvironment(ctx context.Context, clusterID string) (authorization.Environment, error)
}

var (
	ErrConfig            = errors.New("delegation dependencies are required")
	ErrInvalidMint       = errors.New("invalid delegation request")
	ErrInvalidDelegation = errors.New("delegation is missing, expired, already used or does not match the request")
	ErrPolicyUnavailable = errors.New("authorization policy is unavailable")
)
