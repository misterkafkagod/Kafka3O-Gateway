package identity

import (
	"context"
	"io"
	"time"
)

type ListLocalAccounts interface {
	Execute(ctx context.Context, actor LocalAdminActor, afterID string, limit int) (LocalAccountListResult, error)
}

type listLocalAccounts struct {
	store  LocalAccountListStore
	random io.Reader
	now    func() time.Time
}

func NewListLocalAccounts(store LocalAccountListStore, random io.Reader, now func() time.Time) (ListLocalAccounts, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccountAdminConfig
	}
	return listLocalAccounts{store: store, random: random, now: now}, nil
}

func (s listLocalAccounts) Execute(ctx context.Context, actor LocalAdminActor, afterID string, limit int) (LocalAccountListResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountListResult{}, err
	}
	result, err := s.store.ListLocalAccounts(ctx, LocalAccountListRequest{
		Actor: actor, AfterID: afterID, Limit: limit, OperationID: operationID, OccurredAt: s.now(),
	})
	if err != nil {
		return LocalAccountListResult{OperationID: operationID}, err
	}
	result.OperationID = operationID
	return result, nil
}
