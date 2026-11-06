package identity

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"
)

type fakeAccountListStore struct {
	calls   int
	request LocalAccountListRequest
	result  LocalAccountListResult
	err     error
}

func (s *fakeAccountListStore) ListLocalAccounts(_ context.Context, request LocalAccountListRequest) (LocalAccountListResult, error) {
	s.calls++
	s.request = request
	return s.result, s.err
}

func TestListLocalAccountsRejectsMissingDependenciesAndEntropy(t *testing.T) {
	store := &fakeAccountListStore{}
	for _, tt := range []struct {
		store  LocalAccountListStore
		random io.Reader
		now    func() time.Time
	}{
		{nil, rand.Reader, time.Now},
		{store, nil, time.Now},
		{store, rand.Reader, nil},
	} {
		if _, err := NewListLocalAccounts(tt.store, tt.random, tt.now); !errors.Is(err, ErrAccountAdminConfig) {
			t.Fatalf("invalid account listing configuration: %v", err)
		}
	}
	list, err := NewListLocalAccounts(store, failedRandom{}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := list.Execute(context.Background(), LocalAdminActor{}, "", 10); !errors.Is(err, io.ErrUnexpectedEOF) || store.calls != 0 {
		t.Fatalf("entropy failure: err=%v store calls=%d", err, store.calls)
	}
}

func TestListLocalAccountsCorrelatesAndDiscardsFailedResults(t *testing.T) {
	actor := LocalAdminActor{AccountID: "admin", SessionID: "session"}
	store := &fakeAccountListStore{result: LocalAccountListResult{
		OperationID: "untrusted-store-id", Accounts: []LocalAccountSummary{{ID: "user", Username: "example"}},
	}, err: ErrAccountListAudit}
	list, err := NewListLocalAccounts(store, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := list.Execute(context.Background(), actor, "cursor", 10)
	if !errors.Is(err, ErrAccountListAudit) || result.OperationID == "" || result.OperationID != store.request.OperationID ||
		result.Accounts != nil || result.NextCursor != "" || store.request.Actor != actor ||
		store.request.AfterID != "cursor" || store.request.Limit != 10 {
		t.Fatalf("failed listing exposed or lost its operation: %v", err)
	}
	store.err = nil
	result, err = list.Execute(context.Background(), actor, "", 10)
	if err != nil || result.OperationID == "" || result.OperationID != store.request.OperationID ||
		result.OperationID == "untrusted-store-id" || len(result.Accounts) != 1 {
		t.Fatalf("successful listing did not use its generated operation: %v", err)
	}
}
