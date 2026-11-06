package identity

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

type fakeAccountStateStore struct {
	calls int
}

func (s *fakeAccountStateStore) SetLocalAccountEnabled(context.Context, LocalAccountStateChange) error {
	s.calls++
	return nil
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
	if err := change.Execute(context.Background(), LocalAdminActor{}, "subject", false); !errors.Is(err, io.ErrUnexpectedEOF) || store.calls != 0 {
		t.Fatalf("entropy failure: err=%v store calls=%d", err, store.calls)
	}
}
