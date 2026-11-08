package identity

import (
	"context"
	"io"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

type ChangeGroupMapping interface {
	Execute(ctx context.Context, actor LocalAdminActor, issuer, group, clusterID string, expected, role authorization.Role) (AccessChangeResult, error)
}

type changeGroupMapping struct {
	store  GroupMappingStore
	random io.Reader
	now    func() time.Time
}

func NewChangeGroupMapping(store GroupMappingStore, random io.Reader, now func() time.Time) (ChangeGroupMapping, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccessAdminConfig
	}
	return changeGroupMapping{store: store, random: random, now: now}, nil
}

func (c changeGroupMapping) Execute(ctx context.Context, actor LocalAdminActor, issuer, group, clusterID string,
	expected, role authorization.Role) (AccessChangeResult, error) {
	if !validGroupMapping(issuer, group) || !validAccessChange(clusterID, expected, role) {
		return AccessChangeResult{}, ErrInvalidAccessChange
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
	result.PolicyRevision, err = c.store.ChangeGroupMapping(ctx, GroupMappingChange{
		Actor: actor, Issuer: issuer, Group: group, ClusterID: clusterID, ExpectedRole: expected, Role: role,
		OperationID: operationID, EventID: eventID, OccurredAt: c.now(),
	})
	return result, err
}
