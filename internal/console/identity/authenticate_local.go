package identity

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"time"
)

type AuthenticateLocal interface {
	Execute(ctx context.Context, username, password string, client netip.Addr) (LocalAccount, error)
}

type authenticateLocal struct {
	store     LocalAuthenticationStore
	dummyHash string
	random    io.Reader
	now       func() time.Time
	hashSlots chan struct{}
}

func NewAuthenticateLocal(store LocalAuthenticationStore, random io.Reader, now func() time.Time) (AuthenticateLocal, error) {
	if store == nil || random == nil || now == nil {
		return nil, ErrAuthenticationConfig
	}
	dummyHash, err := HashLocalPassword(random, "dummy credential for unknown users")
	if err != nil {
		return nil, fmt.Errorf("initialize local authentication: %w", err)
	}
	return authenticateLocal{store: store, dummyHash: dummyHash, random: random, now: now, hashSlots: make(chan struct{}, 4)}, nil
}

func (a authenticateLocal) Execute(ctx context.Context, username, password string, client netip.Addr) (LocalAccount, error) {
	if !client.IsValid() || client.Zone() != "" {
		return LocalAccount{}, ErrInvalidClientAddress
	}
	name, nameErr := NormalizeLocalUsername(username)
	principal := "invalid"
	if nameErr == nil {
		principal = "user:" + name
	}
	eventID, err := newOpaqueID(a.random)
	if err != nil {
		return LocalAccount{}, err
	}
	attempt := LocalAuthenticationAttempt{
		PrincipalKey: sha256.Sum256([]byte(principal)),
		ClientKey:    sha256.Sum256([]byte("client:" + client.Unmap().String())),
		EventID:      eventID,
		OccurredAt:   a.now(),
	}
	if err := a.store.ReserveLocalAuthentication(ctx, attempt); err != nil {
		return LocalAccount{}, err
	}

	account := LocalAccount{}
	hash := a.dummyHash
	var lookupErr error
	if nameErr == nil {
		account, lookupErr = a.store.FindLocalAccount(ctx, name)
		if lookupErr == nil {
			hash = account.PasswordHash
		} else if errors.Is(lookupErr, ErrAccountNotFound) {
			lookupErr = nil
		}
	}
	matches, verifyErr := verifyLocalPasswordBounded(ctx, a.hashSlots, password, hash)
	if errors.Is(verifyErr, ErrInvalidPassword) {
		verifyErr = nil
	}
	eventID, err = newOpaqueID(a.random)
	if err != nil {
		return LocalAccount{}, err
	}
	result := LocalAuthenticationResult{EventID: eventID, OperationID: attempt.EventID, OccurredAt: a.now()}
	if nameErr == nil && lookupErr == nil && verifyErr == nil && account.ID != "" && matches {
		result.AccountID, result.PasswordHash = account.ID, account.PasswordHash
		result.CredentialVersion = account.CredentialVersion
	}
	authenticated, auditErr := a.store.FinishLocalAuthentication(ctx, result)
	if lookupErr != nil || verifyErr != nil || auditErr != nil {
		return LocalAccount{}, errors.Join(lookupErr, verifyErr, auditErr)
	}
	if !authenticated {
		return LocalAccount{}, ErrAuthenticationFailed
	}
	account.PasswordHash = ""
	account.Enabled = true
	return account, nil
}
