package identity

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"
)

type fakeAdminSessionListStore struct {
	calls   int
	request AdminSessionListRequest
	result  AdminSessionListResult
	err     error
}

func (s *fakeAdminSessionListStore) ListAdminSessions(_ context.Context, request AdminSessionListRequest) (AdminSessionListResult, error) {
	s.calls++
	s.request = request
	return s.result, s.err
}

func TestAdminSessionListingRejectsMissingDependenciesAndEntropy(t *testing.T) {
	store := &fakeAdminSessionListStore{}
	if _, err := NewListAdminSessions(nil, rand.Reader, time.Now); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing store: %v", err)
	}
	if _, err := NewListAdminSessions(store, nil, time.Now); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing random source: %v", err)
	}
	if _, err := NewListAdminSessions(store, rand.Reader, nil); !errors.Is(err, ErrAccountAdminConfig) {
		t.Fatalf("missing clock: %v", err)
	}
	list, err := NewListAdminSessions(store, failedRandom{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := list.Execute(context.Background(), LocalAdminActor{}, "", 1); !errors.Is(err, io.ErrUnexpectedEOF) || store.calls != 0 {
		t.Fatalf("entropy failure: err=%v store calls=%d", err, store.calls)
	}
}

func TestAdminSessionListingCorrelatesAndRedactsStoreFailure(t *testing.T) {
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	store := &fakeAdminSessionListStore{
		result: AdminSessionListResult{Sessions: []AdminSessionSummary{{ID: "not-returned"}}},
		err:    ErrAdminSessionListAudit,
	}
	list, err := NewListAdminSessions(store, rand.Reader, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	actor := LocalAdminActor{AccountID: "admin", SessionID: "actor-session"}
	result, err := list.Execute(context.Background(), actor, "cursor", 10)
	if !errors.Is(err, ErrAdminSessionListAudit) || result.OperationID == "" || result.Sessions != nil ||
		result.NextCursor != "" || result.OperationID != store.request.OperationID || store.request.Actor != actor ||
		store.request.AfterID != "cursor" || store.request.Limit != 10 || !store.request.OccurredAt.Equal(at) || store.calls != 1 {
		t.Fatalf("session list did not safely correlate its store result: %v", err)
	}
	store.err = nil
	result, err = list.Execute(context.Background(), actor, "", 10)
	if err != nil || len(result.Sessions) != 1 || result.OperationID != store.request.OperationID {
		t.Fatalf("session list did not return its store result: %v", err)
	}
}
