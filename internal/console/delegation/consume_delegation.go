package delegation

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type ConsumeDelegation interface {
	Execute(ctx context.Context, request ConsumeRequest) (Decision, error)
}

type consumeDelegation struct {
	store        Store
	grants       SessionGrants
	environments Environments
	protector    ProtectorSource
	random       io.Reader
	now          func() time.Time
}

// NewConsumeDelegation builds the policy decision. protector may be nil, or return nil until Cognito discovery
// succeeds; Cognito sessions then fail closed while local sessions are unaffected.
func NewConsumeDelegation(store Store, grants SessionGrants, environments Environments, protector ProtectorSource,
	random io.Reader, now func() time.Time) (ConsumeDelegation, error) {
	if store == nil || grants == nil || environments == nil || random == nil || now == nil {
		return nil, ErrConfig
	}
	return consumeDelegation{store: store, grants: grants, environments: environments, protector: protector, random: random, now: now}, nil
}

// Execute consumes the delegation (single use, even when denied) and then evaluates current policy for the
// session's current grants and the cluster's current classification. Every outcome after consumption is audited.
func (c consumeDelegation) Execute(ctx context.Context, request ConsumeRequest) (Decision, error) {
	if !validHandle(request.Handle) || request.CallerClusterID == "" || request.Action == "" {
		return Decision{}, ErrInvalidDelegation
	}
	operationID, err := identity.NewOperationID(c.random)
	if err != nil {
		return Decision{}, errors.Join(ErrPolicyUnavailable, err)
	}
	now := c.now()
	consumed, err := c.store.ConsumeGatewayDelegation(ctx, sha256.Sum256([]byte(request.Handle)), now)
	if errors.Is(err, ErrInvalidDelegation) {
		return Decision{OperationID: operationID}, ErrInvalidDelegation
	}
	if err != nil {
		return Decision{OperationID: operationID}, errors.Join(ErrPolicyUnavailable, err)
	}
	record := DecisionRecord{
		OperationID: operationID, OccurredAt: now, ActorID: consumed.AccountID, Source: consumed.Source,
		ClusterID: request.CallerClusterID, Action: request.Action, Resource: consumed.Resource, Reason: consumed.Reason,
	}
	decision := Decision{OperationID: operationID, ActorID: consumed.AccountID, Source: consumed.Source}
	if consumed.ClusterID != request.CallerClusterID || consumed.Action != request.Action ||
		consumed.Binding != request.Binding || !now.Before(consumed.ExpiresAt) {
		record.Denial = "invalid_delegation"
		if err := c.store.RecordGatewayDecision(ctx, record); err != nil {
			return decision, errors.Join(ErrPolicyUnavailable, err)
		}
		return decision, ErrInvalidDelegation
	}
	var protector *cognito.RefreshTokenProtector
	if c.protector != nil {
		protector = c.protector()
	}
	var grants []authorization.Grant
	if consumed.BrowserHash == ([32]byte{}) {
		grants, err = c.grants.ResolveSessionGrants(ctx, consumed.TokenHash, consumed.ClusterID, now, protector)
	} else {
		grants, err = c.grants.ResolveBoundSessionGrants(ctx, consumed.TokenHash, consumed.BrowserHash, consumed.ClusterID, now, protector)
	}
	if errors.Is(err, session.ErrInvalidSession) {
		record.Denial = "invalid_session"
		if err := c.store.RecordGatewayDecision(ctx, record); err != nil {
			return decision, errors.Join(ErrPolicyUnavailable, err)
		}
		return decision, ErrInvalidDelegation
	}
	if err != nil {
		return decision, errors.Join(ErrPolicyUnavailable, err)
	}
	environment, err := c.environments.ClusterEnvironment(ctx, consumed.ClusterID)
	if err != nil {
		return decision, errors.Join(ErrPolicyUnavailable, err)
	}
	record.Environment = environment
	policy, err := authorization.EvaluatePolicy(authorization.PolicyRequest{
		Grants: grants, ClusterID: consumed.ClusterID, Environment: environment, Action: consumed.Action, ChangeReason: consumed.Reason,
	})
	if err != nil {
		return decision, errors.Join(ErrPolicyUnavailable, err)
	}
	record.Allowed, record.Denial = policy.Allowed, string(policy.Denial)
	if err := c.store.RecordGatewayDecision(ctx, record); err != nil {
		return decision, errors.Join(ErrPolicyUnavailable, err)
	}
	decision.Allowed, decision.Denial = policy.Allowed, policy.Denial
	return decision, nil
}
