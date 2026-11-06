package identity

import (
	"context"
	"io"
	"time"
)

type SetLocalAccountEnabled interface {
	Execute(ctx context.Context, actor LocalAdminActor, targetID string, enabled bool) (LocalAccountStateResult, error)
}

type setLocalAccountEnabled struct {
	store  LocalAccountStateStore
	random io.Reader
	now    func() time.Time
}

func NewSetLocalAccountEnabled(store LocalAccountStateStore, random io.Reader, now func() time.Time) (SetLocalAccountEnabled, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccountAdminConfig
	}
	return setLocalAccountEnabled{store: store, random: random, now: now}, nil
}

func (s setLocalAccountEnabled) Execute(ctx context.Context, actor LocalAdminActor, targetID string, enabled bool) (LocalAccountStateResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountStateResult{}, err
	}
	eventID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountStateResult{}, err
	}
	revocationID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountStateResult{}, err
	}
	result := LocalAccountStateResult{OperationID: operationID}
	err = s.store.SetLocalAccountEnabled(ctx, LocalAccountStateChange{
		Actor: actor, TargetID: targetID, Enabled: enabled, OperationID: operationID,
		EventID: eventID, RevocationEventID: revocationID, OccurredAt: s.now(),
	})
	return result, err
}
