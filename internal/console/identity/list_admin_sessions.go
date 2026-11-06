package identity

import (
	"context"
	"io"
	"time"
)

type ListAdminSessions interface {
	Execute(ctx context.Context, actor LocalAdminActor, afterID string, limit int) (AdminSessionListResult, error)
}

type listAdminSessions struct {
	store  AdminSessionListStore
	random io.Reader
	now    func() time.Time
}

func NewListAdminSessions(store AdminSessionListStore, random io.Reader, now func() time.Time) (ListAdminSessions, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccountAdminConfig
	}
	return listAdminSessions{store: store, random: random, now: now}, nil
}

func (s listAdminSessions) Execute(ctx context.Context, actor LocalAdminActor, afterID string, limit int) (AdminSessionListResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return AdminSessionListResult{}, err
	}
	result, err := s.store.ListAdminSessions(ctx, AdminSessionListRequest{
		Actor: actor, AfterID: afterID, Limit: limit, OperationID: operationID, OccurredAt: s.now(),
	})
	if err != nil {
		return AdminSessionListResult{OperationID: operationID}, err
	}
	result.OperationID = operationID
	return result, nil
}
