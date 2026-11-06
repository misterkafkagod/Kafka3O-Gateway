package identity

import (
	"context"
	"io"
	"time"
)

type ResetLocalPassword interface {
	Execute(ctx context.Context, actor LocalAdminActor, targetID, temporaryPassword string) (PasswordResetResult, error)
}

type resetLocalPassword struct {
	store     LocalPasswordResetStore
	random    io.Reader
	now       func() time.Time
	hashSlots chan struct{}
}

func NewResetLocalPassword(store LocalPasswordResetStore, random io.Reader, now func() time.Time) (ResetLocalPassword, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccountAdminConfig
	}
	return resetLocalPassword{store: store, random: random, now: now, hashSlots: make(chan struct{}, 4)}, nil
}

func (s resetLocalPassword) Execute(ctx context.Context, actor LocalAdminActor, targetID, temporaryPassword string) (PasswordResetResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return PasswordResetResult{}, err
	}
	eventID, err := newOpaqueID(s.random)
	if err != nil {
		return PasswordResetResult{}, err
	}
	revocationID, err := newOpaqueID(s.random)
	if err != nil {
		return PasswordResetResult{}, err
	}
	result := PasswordResetResult{OperationID: operationID}
	attempt := LocalPasswordResetAttempt{Actor: actor, TargetID: targetID, OperationID: operationID, OccurredAt: s.now()}
	if err := s.store.ReserveLocalPasswordReset(ctx, attempt); err != nil {
		return result, err
	}
	if err := ValidateLocalPassword(temporaryPassword); err != nil {
		return result, s.failLocalPasswordReset(ctx, attempt, eventID, err)
	}
	if err := ctx.Err(); err != nil {
		return result, s.failLocalPasswordReset(ctx, attempt, eventID, err)
	}
	select {
	case s.hashSlots <- struct{}{}:
		defer func() { <-s.hashSlots }()
	case <-ctx.Done():
		return result, s.failLocalPasswordReset(ctx, attempt, eventID, ctx.Err())
	}
	hash, err := HashLocalPassword(s.random, temporaryPassword)
	if err != nil {
		return result, s.failLocalPasswordReset(ctx, attempt, eventID, err)
	}
	err = s.store.CompleteLocalPasswordReset(ctx, LocalPasswordResetCompletion{
		Actor: actor, TargetID: targetID, OperationID: operationID, EventID: eventID,
		RevocationEventID: revocationID, NewHash: hash, OccurredAt: s.now(),
	})
	return result, err
}
