package identity

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"
)

type fakeAccountStateStore struct {
	calls  int
	change LocalAccountStateChange
	err    error
}

func (s *fakeAccountStateStore) SetLocalAccountEnabled(_ context.Context, change LocalAccountStateChange) error {
	s.calls++
	s.change = change
	return s.err
}

func TestSetLocalAccountEnabledRejectsMissingDependenciesAndEntropy(t *testing.T) {
	store := &fakeAccountStateStore{}
	if _, err := NewSetLocalAccountEnabled(nil, failedRandom{}, time.Now); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing store: %v", err)
	}
	if _, err := NewSetLocalAccountEnabled(store, nil, time.Now); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing entropy source: %v", err)
	}
	if _, err := NewSetLocalAccountEnabled(store, failedRandom{}, nil); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing clock: %v", err)
	}
	change, err := NewSetLocalAccountEnabled(store, failedRandom{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := change.Execute(context.Background(), LocalAdminActor{}, "subject", false); !errors.Is(err, io.ErrUnexpectedEOF) || store.calls != 0 {
		t.Fatalf("entropy failure: err=%v store calls=%d", err, store.calls)
	}
}

func TestSetLocalAccountEnabledCorrelatesStoreResult(t *testing.T) {
	store := &fakeAccountStateStore{err: ErrAccountStateUnknown}
	change, err := NewSetLocalAccountEnabled(store, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := LocalAdminActor{AccountID: "admin", SessionID: "session"}
	result, err := change.Execute(context.Background(), actor, "subject", false)
	if !errors.Is(err, ErrAccountStateUnknown) || result.OperationID == "" ||
		result.OperationID != store.change.OperationID || store.change.Actor != actor ||
		store.change.TargetID != "subject" || store.change.Enabled {
		t.Fatalf("state change did not correlate its store result: %v", err)
	}
}
