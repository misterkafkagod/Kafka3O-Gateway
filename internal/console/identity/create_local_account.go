package identity

import (
	"context"
	"io"
	"time"
)

type CreateLocalAccount interface {
	Execute(ctx context.Context, actor LocalAdminActor, username, temporaryPassword string) (LocalAccountCreationResult, error)
}

type createLocalAccount struct {
	store     LocalAccountCreationStore
	random    io.Reader
	now       func() time.Time
	hashSlots chan struct{}
}

func NewCreateLocalAccount(store LocalAccountCreationStore, random io.Reader, now func() time.Time) (CreateLocalAccount, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAccountAdminConfig
	}
	return createLocalAccount{store: store, random: random, now: now, hashSlots: make(chan struct{}, 4)}, nil
}

func (s createLocalAccount) Execute(ctx context.Context, actor LocalAdminActor, username, temporaryPassword string) (LocalAccountCreationResult, error) {
	operationID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountCreationResult{}, err
	}
	eventID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountCreationResult{}, err
	}
	accountID, err := newOpaqueID(s.random)
	if err != nil {
		return LocalAccountCreationResult{}, err
	}
	result := LocalAccountCreationResult{OperationID: operationID}
	attempt := LocalAccountCreationAttempt{Actor: actor, AccountID: accountID, OperationID: operationID, OccurredAt: s.now()}
	if err := s.store.ReserveLocalAccountCreation(ctx, attempt); err != nil {
		return result, err
	}
	name, err := NormalizeLocalUsername(username)
	if err != nil {
		return result, s.failLocalAccountCreation(ctx, attempt, eventID, err)
	}
	if err := ValidateLocalPassword(temporaryPassword); err != nil {
		return result, s.failLocalAccountCreation(ctx, attempt, eventID, err)
	}
	if err := ctx.Err(); err != nil {
		return result, s.failLocalAccountCreation(ctx, attempt, eventID, err)
	}
	select {
	case s.hashSlots <- struct{}{}:
		defer func() { <-s.hashSlots }()
	case <-ctx.Done():
		return result, s.failLocalAccountCreation(ctx, attempt, eventID, ctx.Err())
	}
	hash, err := HashLocalPassword(s.random, temporaryPassword)
	if err != nil {
		return result, s.failLocalAccountCreation(ctx, attempt, eventID, err)
	}
	account := LocalAccount{ID: accountID, Username: name, PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := s.store.CompleteLocalAccountCreation(ctx, LocalAccountCreationCompletion{
		Actor: actor, Account: account, OperationID: operationID, EventID: eventID, OccurredAt: s.now(),
	}); err != nil {
		return result, err
	}
	account.PasswordHash = ""
	result.Account = account
	return result, nil
}
