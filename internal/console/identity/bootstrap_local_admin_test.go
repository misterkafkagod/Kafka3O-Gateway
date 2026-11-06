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
	calls       int
	account     LocalAccount
	eventID     string
	operationID string
	at          time.Time
	attempts    []BootstrapAttempt
	failures    []BootstrapFailure
	err         error
	reserveErr  error
	failureErr  error
}

func (f *fakeInitialAdminStore) ReserveBootstrapAttempt(_ context.Context, attempt BootstrapAttempt) error {
	f.attempts = append(f.attempts, attempt)
	return f.reserveErr
}

func (f *fakeInitialAdminStore) FailBootstrapAttempt(_ context.Context, failure BootstrapFailure) error {
	f.failures = append(f.failures, failure)
	return f.failureErr
}

func (f *fakeInitialAdminStore) CreateInitialAdmin(_ context.Context, account LocalAccount, eventID, operationID string, at time.Time) error {
	f.calls++
	f.account, f.eventID, f.operationID, f.at = account, eventID, operationID, at
	return f.err
}

func TestBootstrapLocalAdminRequiresSecretAndCreatesMustChangeAccount(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	store := &fakeInitialAdminStore{}
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	bootstrap, err := NewBootstrapLocalAdmin(store, secret, bytes.NewReader(bytes.Repeat([]byte{9}, 96)), func() time.Time { return now })
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
	if store.calls != 1 || store.account.ID != account.ID || store.eventID == "" || store.operationID == "" ||
		store.at != now || len(store.attempts) != 2 || len(store.failures) != 1 ||
		store.failures[0].OperationID != store.attempts[0].EventID {
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
	bootstrap, err = NewBootstrapLocalAdmin(store, secret, bytes.NewReader(make([]byte, 64)), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := bootstrap.Execute(context.Background(), secret, "admin", "a strong passphrase")
	if !errors.Is(err, ErrAlreadyBootstrapped) || account != (LocalAccount{}) {
		t.Fatalf("store failure = %v; account = %+v", err, account)
	}
}

func TestBootstrapLocalAdminFailsClosedWhenAttemptOrFailureAuditFails(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	auditErr := errors.New("audit unavailable")
	for _, tt := range []struct {
		name       string
		reserveErr error
		failureErr error
		want       error
	}{
		{"reservation", auditErr, nil, auditErr},
		{"throttled", ErrBootstrapThrottled, nil, ErrBootstrapThrottled},
		{"failed outcome", nil, auditErr, auditErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeInitialAdminStore{reserveErr: tt.reserveErr, failureErr: tt.failureErr}
			bootstrap, err := NewBootstrapLocalAdmin(store, secret, bytes.NewReader(make([]byte, 32)), time.Now)
			if err != nil {
				t.Fatal(err)
			}
			account, err := bootstrap.Execute(context.Background(), "incorrect", "admin", "initial passphrase")
			if !errors.Is(err, tt.want) || account != (LocalAccount{}) || store.calls != 0 {
				t.Fatalf("account=%+v error=%v create calls=%d", account, err, store.calls)
			}
			if tt.failureErr != nil && !errors.Is(err, ErrInvalidBootstrapSecret) {
				t.Fatalf("original bootstrap failure was lost: %v", err)
			}
		})
	}
}
