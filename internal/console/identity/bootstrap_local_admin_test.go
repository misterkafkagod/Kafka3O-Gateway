package identity

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"testing"
	"time"
)

type fakeInitialAdminStore struct {
	calls   int
	account LocalAccount
	eventID string
	at      time.Time
	err     error
}

func (f *fakeInitialAdminStore) CreateInitialAdmin(_ context.Context, account LocalAccount, eventID string, at time.Time) error {
	f.calls++
	f.account, f.eventID, f.at = account, eventID, at
	return f.err
}

func TestBootstrapLocalAdminRequiresSecretAndCreatesMustChangeAccount(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	store := &fakeInitialAdminStore{}
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	bootstrap, err := NewBootstrapLocalAdmin(store, secret, bytes.NewReader(bytes.Repeat([]byte{9}, 48)), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(context.Background(), "incorrect", "Admin", "a strong passphrase"); !errors.Is(err, ErrInvalidBootstrapSecret) || store.calls != 0 {
		t.Fatalf("wrong secret: err=%v calls=%d", err, store.calls)
	}
	account, err := bootstrap.Execute(context.Background(), secret, "Admin", "a strong passphrase")
	if err != nil {
		t.Fatal(err)
	}
	if account.Username != "admin" || account.ID == "" || !account.Enabled || !account.MustChangePassword || account.PasswordHash != "" {
		t.Fatalf("returned account = %+v", account)
	}
	if store.calls != 1 || store.account.ID != account.ID || store.eventID == "" || store.at != now {
		t.Fatalf("store was not called with the expected identity and audit metadata")
	}
	matches, err := VerifyLocalPassword("a strong passphrase", store.account.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("stored hash did not verify: matches=%t err=%v", matches, err)
	}
}

func TestBootstrapLocalAdminRejectsWeakConfiguration(t *testing.T) {
	validSecret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	now := time.Now
	store := &fakeInitialAdminStore{}
	random := bytes.NewReader(make([]byte, 48))
	for _, tt := range []struct {
		store  InitialAdminStore
		secret string
		random *bytes.Reader
		now    func() time.Time
	}{
		{store, "short", random, now},
		{nil, validSecret, random, now},
		{store, validSecret, nil, now},
		{store, validSecret, random, nil},
	} {
		var source = io.Reader(tt.random)
		if tt.random == nil {
			source = nil
		}
		if _, err := NewBootstrapLocalAdmin(tt.store, tt.secret, source, tt.now); !errors.Is(err, ErrInvalidBootstrapConfig) {
			t.Errorf("invalid configuration: %v", err)
		}
	}
}

func TestBootstrapLocalAdminPropagatesEntropyAndStoreFailures(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	store := &fakeInitialAdminStore{}
	bootstrap, err := NewBootstrapLocalAdmin(store, secret, failedRandom{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(context.Background(), secret, "admin", "a strong passphrase"); !errors.Is(err, io.ErrUnexpectedEOF) || store.calls != 0 {
		t.Fatalf("entropy failure = %v; store calls = %d", err, store.calls)
	}
	store.err = ErrAlreadyBootstrapped
	bootstrap, err = NewBootstrapLocalAdmin(store, secret, bytes.NewReader(make([]byte, 48)), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := bootstrap.Execute(context.Background(), secret, "admin", "a strong passphrase")
	if !errors.Is(err, ErrAlreadyBootstrapped) || account != (LocalAccount{}) {
		t.Fatalf("store failure = %v; account = %+v", err, account)
	}
}
