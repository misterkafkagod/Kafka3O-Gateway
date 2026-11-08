// Package gatewayauth enforces delegated Console authorization in a Gateway's secured mode.
package gatewayauth

import (
	"context"
	"errors"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

// Route is the authorization classification of one Gateway request.
type Route struct {
	Action    authorization.Action
	Anonymous bool
}

// DelegationHeader carries the Console's single-use delegation handle.
const DelegationHeader = "X-Kafka3O-Delegation"

// MaxBodyBytes bounds the request body read for the binding; larger requests are rejected before policy.
const MaxBodyBytes = 1 << 20

type PolicyDecision struct {
	Allowed     bool
	ActorID     string
	Source      string
	Denial      string
	OperationID string
}

// PolicyClient asks the Console for a decision; it never retries.
type PolicyClient interface {
	Consume(ctx context.Context, handle string, binding [32]byte, action authorization.Action) (PolicyDecision, error)
}

var (
	ErrInvalidDelegation = errors.New("the Console rejected the delegation")
	ErrPolicyUnavailable = errors.New("the Console policy service is unavailable")
	ErrConfig            = errors.New("secured-mode dependencies are required")
)
