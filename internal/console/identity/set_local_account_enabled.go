package identity

import (
	"context"
	"io"
	"time"
)

type SetLocalAccountEnabled interface {
	Execute(ctx context.Context, actor LocalAdminActor, targetID string, enabled bool) error
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

func (s setLocalAccountEnabled) Execute(ctx context.Context, actor LocalAdminActor, targetID string, enabled bool) error {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return err
	}
	eventID, err := newOpaqueID(s.random)
	if err != nil {
		return err
	}
	revocationID, err := newOpaqueID(s.random)
	if err != nil {
		return err
	}
	return s.store.SetLocalAccountEnabled(ctx, LocalAccountStateChange{
		Actor: actor, TargetID: targetID, Enabled: enabled, OperationID: operationID,
		EventID: eventID, RevocationEventID: revocationID, OccurredAt: s.now(),
	})
}
