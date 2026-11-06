package identity

import (
	"context"
	"io"
	"time"
)

type RevokeAdminSession interface {
	Execute(ctx context.Context, actor LocalAdminActor, sessionID string) (AdminSessionRevocationResult, error)
}

type revokeAdminSession struct {
	store  AdminSessionRevocationStore
	random io.Reader
	now    func() time.Time
}

func NewRevokeAdminSession(store AdminSessionRevocationStore, random io.Reader, now func() time.Time) (RevokeAdminSession, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccountAdminConfig
	}
	return revokeAdminSession{store: store, random: random, now: now}, nil
}

func (s revokeAdminSession) Execute(ctx context.Context, actor LocalAdminActor, sessionID string) (AdminSessionRevocationResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return AdminSessionRevocationResult{}, err
	}
	eventID, err := newOpaqueID(s.random)
	if err != nil {
		return AdminSessionRevocationResult{}, err
	}
	result := AdminSessionRevocationResult{OperationID: operationID}
	err = s.store.RevokeAdminSession(ctx, AdminSessionRevocation{
		Actor: actor, SessionID: sessionID, OperationID: operationID, EventID: eventID, OccurredAt: s.now(),
	})
	return result, err
}
