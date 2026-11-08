package identity

import (
	"context"
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

type ChangeClusterEnvironment interface {
	Execute(ctx context.Context, actor LocalAdminActor, clusterID string, expected, environment authorization.Environment, reason string) (AccessChangeResult, error)
}

type changeClusterEnvironment struct {
	store  ClusterEnvironmentStore
	random io.Reader
	now    func() time.Time
}

func NewChangeClusterEnvironment(store ClusterEnvironmentStore, random io.Reader, now func() time.Time) (ChangeClusterEnvironment, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccessAdminConfig
	}
	return changeClusterEnvironment{store: store, random: random, now: now}, nil
}

func (c changeClusterEnvironment) Execute(ctx context.Context, actor LocalAdminActor, clusterID string,
	expected, environment authorization.Environment, reason string) (AccessChangeResult, error) {
	switch expected {
	case authorization.EnvironmentUnclassified, authorization.EnvironmentNonProduction, authorization.EnvironmentProduction:
	default:
		return AccessChangeResult{}, ErrInvalidAccessChange
	}
	if !validClusterID(clusterID) || (environment != authorization.EnvironmentNonProduction && environment != authorization.EnvironmentProduction) {
		return AccessChangeResult{}, ErrInvalidAccessChange
	}
	if !validChangeReason(reason) {
		return AccessChangeResult{}, ErrChangeReasonRequired
	}
	operationID, err := newOpaqueID(c.random)
	if err != nil {
		return AccessChangeResult{}, err
	}
	eventID, err := newOpaqueID(c.random)
	if err != nil {
		return AccessChangeResult{}, err
	}
	result := AccessChangeResult{OperationID: operationID}
	result.PolicyRevision, err = c.store.ChangeClusterEnvironment(ctx, ClusterEnvironmentChange{
		Actor: actor, ClusterID: clusterID, ExpectedEnvironment: expected, Environment: environment, Reason: reason,
		OperationID: operationID, EventID: eventID, OccurredAt: c.now(),
	})
	return result, err
}
