package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

type boundCoordinatorTestStore struct {
	testStore
	*deadlineRefreshStore
	useErr error
}

func (s *boundCoordinatorTestStore) UseBoundSession(context.Context, [32]byte, [32]byte, func() time.Time, time.Duration, *cognito.RefreshTokenProtector) (Principal, error) {
	return Principal{}, s.useErr
}

func (s *boundCoordinatorTestStore) InspectBoundCognitoRefresh(ctx context.Context, tokenHash, _ [32]byte, now time.Time) (CognitoRefreshState, error) {
	return s.InspectCognitoRefresh(ctx, tokenHash, now)
}

func (*boundCoordinatorTestStore) ReserveBoundCognitoRefresh(context.Context, cognito.RefreshReservationRequest, [32]byte) (cognito.RefreshReservation, error) {
	return cognito.RefreshReservation{}, errors.New("unexpected bound reservation")
}

func (*boundCoordinatorTestStore) ValidateBoundCognitoRefreshDispatch(context.Context, cognito.BoundRefreshDispatchRequest) error {
	return errors.New("unexpected bound dispatch")
}

func (*boundCoordinatorTestStore) RecordBoundCognitoRefreshFailure(context.Context, cognito.RefreshFailure, [32]byte) error {
	return errors.New("unexpected bound failure audit")
}

func (*boundCoordinatorTestStore) CompleteBoundCognitoRefresh(context.Context, cognito.RefreshCompletionRequest, [32]byte) error {
	return errors.New("unexpected bound completion")
}

func TestBoundCognitoSessionRefreshCoordinatorSharesGlobalBounds(t *testing.T) {
	store := &boundCoordinatorTestStore{deadlineRefreshStore: &deadlineRefreshStore{}}
	coordinator := &cognitoRefreshCoordinator{store: store, active: make(map[cognitoRefreshKey]*cognitoRefreshCall)}
	manager := &Manager{store: store, now: time.Now, cognitoRefresh: coordinator}
	for i := 1; i <= 32; i++ {
		key := cognitoRefreshKey{tokenHash: [32]byte{byte(i)}}
		if i > 16 {
			key.browserHash = [32]byte{1}
		}
		coordinator.active[key] = &cognitoRefreshCall{done: make(chan struct{})}
	}
	if _, err := manager.resolveCognitoRefreshWithBrowserBinding(context.Background(), [32]byte{33}, [32]byte{1}); !errors.Is(err, ErrCognitoRefreshUnavailable) {
		t.Fatalf("bound work exceeded the shared refresh limit: %v", err)
	}
	if _, err := manager.resolveCognitoRefreshWithBrowserBinding(context.Background(), [32]byte{33}, [32]byte{}); !errors.Is(err, ErrCognitoRefreshUnavailable) {
		t.Fatalf("unbound work exceeded the shared refresh limit: %v", err)
	}
	waiting, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.resolveCognitoRefreshWithBrowserBinding(waiting, [32]byte{17}, [32]byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("bound waiter did not observe cancellation: %v", err)
	}
	delete(coordinator.active, cognitoRefreshKey{tokenHash: [32]byte{32}, browserHash: [32]byte{1}})
	started := time.Now()
	if _, err := manager.resolveCognitoRefreshWithBrowserBinding(context.Background(), [32]byte{33}, [32]byte{1}); err != ErrInvalidSession {
		t.Fatalf("released bound slot did not run inspection: %v", err)
	}
	if store.deadline.Before(started.Add(14*time.Second)) || store.deadline.After(started.Add(16*time.Second)) {
		t.Fatal("bound refresh did not use the shared 15-second deadline")
	}
}

func TestBoundCognitoSessionRefreshDoesNotStartAfterUncertainUse(t *testing.T) {
	failure := errors.Join(ErrInvalidSession, errors.New("rollback failed"))
	store := &boundCoordinatorTestStore{deadlineRefreshStore: &deadlineRefreshStore{}, useErr: failure}
	manager := &Manager{
		store: store, now: time.Now, idleLifetime: time.Minute,
		cognitoRefresh: &cognitoRefreshCoordinator{store: store, active: make(map[cognitoRefreshKey]*cognitoRefreshCall)},
	}
	token := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if principal, err := manager.ResolveBound(context.Background(), token, [32]byte{1}); err != failure || principal.SessionID != "" || !store.deadline.IsZero() {
		t.Fatalf("uncertain bound validation triggered refresh or lost its error: %v", err)
	}
}

func TestBoundCognitoSessionRefreshPreservesUncertainInspectionAndWaiterErrors(t *testing.T) {
	failure := errors.Join(ErrInvalidSession, ErrBrowserSessionUnknown)
	store := &boundCoordinatorTestStore{deadlineRefreshStore: &deadlineRefreshStore{inspectErr: failure}, useErr: ErrInvalidSession}
	coordinator := &cognitoRefreshCoordinator{store: store, active: make(map[cognitoRefreshKey]*cognitoRefreshCall)}
	manager := &Manager{store: store, now: time.Now, idleLifetime: time.Minute, cognitoRefresh: coordinator}
	token := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if principal, err := manager.ResolveBound(context.Background(), token, [32]byte{1}); err == ErrInvalidSession ||
		!errors.Is(err, ErrCognitoRefreshUnavailable) || !errors.Is(err, ErrBrowserSessionUnknown) || principal.SessionID != "" {
		t.Fatalf("uncertain bound inspection became a definite invalid session: %v", err)
	}
	hash, err := hashSessionToken(token)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	close(done)
	coordinator.mu.Lock()
	coordinator.active[cognitoRefreshKey{tokenHash: hash, browserHash: [32]byte{1}}] = &cognitoRefreshCall{done: done}
	coordinator.mu.Unlock()
	if principal, err := manager.ResolveBound(context.Background(), token, [32]byte{1}); err == ErrInvalidSession ||
		!errors.Is(err, ErrCognitoRefreshUnavailable) || !errors.Is(err, ErrBrowserSessionUnknown) || principal.SessionID != "" {
		t.Fatalf("uncertain bound waiter revalidation became a definite invalid session: %v", err)
	}
}
