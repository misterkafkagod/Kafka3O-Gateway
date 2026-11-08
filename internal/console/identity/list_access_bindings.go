package identity

import (
	"context"
	"io"
	"time"
)

type ListAccessBindings interface {
	Execute(ctx context.Context, actor LocalAdminActor, afterID string, limit int) (AccessListResult, error)
}

type listAccessBindings struct {
	store  AccessListStore
	random io.Reader
	now    func() time.Time
}

func NewListAccessBindings(store AccessListStore, random io.Reader, now func() time.Time) (ListAccessBindings, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccessAdminConfig
	}
	return listAccessBindings{store: store, random: random, now: now}, nil
}

func (s listAccessBindings) Execute(ctx context.Context, actor LocalAdminActor, afterID string, limit int) (AccessListResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return AccessListResult{}, err
	}
	result, err := s.store.ListAccessBindings(ctx, AccessListRequest{
		Actor: actor, AfterID: afterID, Limit: limit, OperationID: operationID, OccurredAt: s.now(),
	})
	if err != nil {
		return AccessListResult{OperationID: operationID}, err
	}
	result.OperationID = operationID
	return result, nil
}
