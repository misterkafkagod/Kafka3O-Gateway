package identity

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

type testAuthenticationStore struct{}

func (testAuthenticationStore) FindLocalAccount(context.Context, string) (LocalAccount, error) {
	return LocalAccount{}, ErrAccountNotFound
}

func (testAuthenticationStore) ReserveLocalAuthentication(context.Context, LocalAuthenticationAttempt) error {
	return nil
}

func (testAuthenticationStore) FinishLocalAuthentication(context.Context, LocalAuthenticationResult) (bool, error) {
	return false, nil
}

func (testAuthenticationStore) UpdateLocalPassword(context.Context, LocalPasswordUpdate) error {
	return nil
}

func TestBoundedPasswordVerificationHonorsCancellation(t *testing.T) {
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := verifyLocalPasswordBounded(ctx, slots, "password", "hash"); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked verification: %v", err)
	}
	if len(slots) != 1 {
		t.Fatal("blocked verification altered the occupied slot")
	}
}

func TestLocalCredentialWorkHasBoundedConcurrency(t *testing.T) {
	store := testAuthenticationStore{}
	authenticator, err := NewAuthenticateLocal(store, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if got := cap(authenticator.(authenticateLocal).hashSlots); got != 4 {
		t.Fatalf("authentication work slots = %d, want 4", got)
	}
	change, err := NewChangeLocalPassword(authenticator, store, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if got := cap(change.(changeLocalPassword).hashSlots); got != 4 {
		t.Fatalf("password-change work slots = %d, want 4", got)
	}
}
