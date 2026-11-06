package identity

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"io"
	"time"
)

type BootstrapLocalAdmin interface {
	Execute(ctx context.Context, secret, username, password string) (LocalAccount, error)
}

type bootstrapLocalAdmin struct {
	store      InitialAdminStore
	secretHash [sha256.Size]byte
	random     io.Reader
	now        func() time.Time
	hashSlots  chan struct{}
}

func NewBootstrapLocalAdmin(store InitialAdminStore, secret string, random io.Reader, now func() time.Time) (BootstrapLocalAdmin, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(secret)
	if err != nil || len(decoded) < 32 || len(decoded) > 64 ||
		base64.RawURLEncoding.EncodeToString(decoded) != secret || store == nil || random == nil || now == nil {
		return nil, ErrInvalidBootstrapConfig
	}
	return bootstrapLocalAdmin{
		store: store, secretHash: sha256.Sum256([]byte(secret)), random: random, now: now, hashSlots: make(chan struct{}, 4),
	}, nil
}

func (b bootstrapLocalAdmin) Execute(ctx context.Context, secret, username, password string) (LocalAccount, error) {
	attemptID, err := newOpaqueID(b.random)
	if err != nil {
		return LocalAccount{}, err
	}
	resultID, err := newOpaqueID(b.random)
	if err != nil {
		return LocalAccount{}, err
	}
	if err := b.store.ReserveBootstrapAttempt(ctx, BootstrapAttempt{EventID: attemptID, OccurredAt: b.now()}); err != nil {
		return LocalAccount{}, err
	}
	if len(secret) > 128 {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, ErrInvalidBootstrapSecret)
	}
	got := sha256.Sum256([]byte(secret))
	if subtle.ConstantTimeCompare(got[:], b.secretHash[:]) != 1 {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, ErrInvalidBootstrapSecret)
	}
	name, err := NormalizeLocalUsername(username)
	if err != nil {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, err)
	}
	if err := ValidateLocalPassword(password); err != nil {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, err)
	}
	id, err := newOpaqueID(b.random)
	if err != nil {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, err)
	}
	if err := ctx.Err(); err != nil {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, err)
	}
	select {
	case b.hashSlots <- struct{}{}:
		defer func() { <-b.hashSlots }()
	case <-ctx.Done():
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, ctx.Err())
	}
	hash, err := HashLocalPassword(b.random, password)
	if err != nil {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, err)
	}
	account := LocalAccount{ID: id, Username: name, PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := b.store.CreateInitialAdmin(ctx, account, resultID, attemptID, b.now()); err != nil {
		return LocalAccount{}, b.failBootstrapAttempt(ctx, resultID, attemptID, err)
	}
	account.PasswordHash = ""
	return account, nil
}
