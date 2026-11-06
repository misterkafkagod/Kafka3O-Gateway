package identity

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"net/netip"
	"time"
)

type ChangeLocalPassword interface {
	Execute(ctx context.Context, accountID, username, currentPassword, newPassword string, client netip.Addr) error
}

type changeLocalPassword struct {
	authenticator AuthenticateLocal
	store         LocalPasswordStore
	random        io.Reader
	now           func() time.Time
	hashSlots     chan struct{}
}

func NewChangeLocalPassword(authenticator AuthenticateLocal, store LocalPasswordStore, random io.Reader, now func() time.Time) (ChangeLocalPassword, error) {
	if authenticator == nil || store == nil || random == nil || now == nil {
		return nil, ErrAuthenticationConfig
	}
	return changeLocalPassword{authenticator: authenticator, store: store, random: random, now: now, hashSlots: make(chan struct{}, 4)}, nil
}

func (c changeLocalPassword) Execute(ctx context.Context, accountID, username, currentPassword, newPassword string, client netip.Addr) error {
	if err := ValidateLocalPassword(newPassword); err != nil {
		return err
	}
	account, err := c.authenticator.Execute(ctx, username, currentPassword, client)
	if err != nil {
		return err
	}
	if accountID == "" || account.ID != accountID {
		return ErrAuthenticationFailed
	}
	if subtle.ConstantTimeCompare([]byte(currentPassword), []byte(newPassword)) == 1 {
		return ErrPasswordUnchanged
	}
	stored, err := c.store.FindLocalAccount(ctx, account.Username)
	if errors.Is(err, ErrAccountNotFound) {
		return ErrAuthenticationFailed
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.hashSlots <- struct{}{}:
		defer func() { <-c.hashSlots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	matches, err := VerifyLocalPassword(currentPassword, stored.PasswordHash)
	if err != nil {
		return err
	}
	if stored.ID != accountID || !stored.Enabled || !matches {
		return ErrAuthenticationFailed
	}
	newHash, err := HashLocalPassword(c.random, newPassword)
	if err != nil {
		return err
	}
	eventID, err := newOpaqueID(c.random)
	if err != nil {
		return err
	}
	revocationEventID, err := newOpaqueID(c.random)
	if err != nil {
		return err
	}
	return c.store.UpdateLocalPassword(ctx, LocalPasswordUpdate{
		AccountID: accountID, PreviousHash: stored.PasswordHash, NewHash: newHash,
		EventID: eventID, RevocationEventID: revocationEventID, OccurredAt: c.now(),
	})
}
