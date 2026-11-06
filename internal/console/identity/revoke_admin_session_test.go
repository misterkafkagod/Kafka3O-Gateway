package identity

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type fakeAdminSessionRevocationStore struct {
	calls      int
	revocation AdminSessionRevocation
	err        error
}

func (s *fakeAdminSessionRevocationStore) RevokeAdminSession(_ context.Context, revocation AdminSessionRevocation) error {
	s.calls++
	s.revocation = revocation
	return s.err
}

func TestSessionIDValidation(t *testing.T) {
	valid := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))
	if err := ValidateSessionID(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		"", valid + "=", valid[:21], valid + "A", strings.Repeat("A", 21) + "B",
		strings.Repeat("A", 21) + "/", strings.Repeat("A", 21) + "\n",
	} {
		if !errors.Is(ValidateSessionID(invalid), ErrInvalidSessionID) {
			t.Fatal("invalid session identifier was accepted")
		}
	}
}

func TestAdminSessionRevocationRejectsMissingDependenciesAndEntropy(t *testing.T) {
	store := &fakeAdminSessionRevocationStore{}
	if _, err := NewRevokeAdminSession(nil, rand.Reader, time.Now); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing store: %v", err)
	}
	if _, err := NewRevokeAdminSession(store, nil, time.Now); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing random source: %v", err)
	}
	if _, err := NewRevokeAdminSession(store, rand.Reader, nil); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing clock: %v", err)
	}
	revoke, err := NewRevokeAdminSession(store, failedRandom{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := revoke.Execute(context.Background(), LocalAdminActor{}, "target"); !errors.Is(err, io.ErrUnexpectedEOF) || store.calls != 0 {
		t.Fatalf("entropy failure: err=%v store calls=%d", err, store.calls)
	}
}

func TestAdminSessionRevocationCorrelatesStoreResult(t *testing.T) {
	store := &fakeAdminSessionRevocationStore{err: ErrAdminSessionRevokeUnknown}
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	revoke, err := NewRevokeAdminSession(store, rand.Reader, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	actor := LocalAdminActor{AccountID: "admin", SessionID: "actor-session"}
	result, err := revoke.Execute(context.Background(), actor, "target-session")
	if !errors.Is(err, ErrAdminSessionRevokeUnknown) || result.OperationID == "" ||
		result.OperationID != store.revocation.OperationID || store.revocation.EventID == "" ||
		store.revocation.EventID == result.OperationID || store.revocation.Actor != actor ||
		store.revocation.SessionID != "target-session" || !store.revocation.OccurredAt.Equal(at) || store.calls != 1 {
		t.Fatalf("session revocation did not correlate its store result: %v", err)
	}
}
