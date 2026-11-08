package identity

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
)

type ChangeRoleGrant interface {
	Execute(ctx context.Context, actor LocalAdminActor, targetID, clusterID string, expected, role authorization.Role) (AccessChangeResult, error)
}

type changeRoleGrant struct {
	store  RoleGrantStore
	random io.Reader
	now    func() time.Time
}

func NewChangeRoleGrant(store RoleGrantStore, random io.Reader, now func() time.Time) (ChangeRoleGrant, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccessAdminConfig
	}
	return changeRoleGrant{store: store, random: random, now: now}, nil
}

func (c changeRoleGrant) Execute(ctx context.Context, actor LocalAdminActor, targetID, clusterID string,
	expected, role authorization.Role) (AccessChangeResult, error) {
	if strings.TrimSpace(targetID) == "" || len(targetID) > 128 || !validAccessChange(clusterID, expected, role) {
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
	result.PolicyRevision, err = c.store.ChangeRoleGrant(ctx, RoleGrantChange{
		Actor: actor, TargetID: targetID, ClusterID: clusterID, ExpectedRole: expected, Role: role,
		OperationID: operationID, EventID: eventID, OccurredAt: c.now(),
	})
	return result, err
}
