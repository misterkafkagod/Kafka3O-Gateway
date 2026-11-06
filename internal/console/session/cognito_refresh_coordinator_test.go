package session

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

type deadlineRefreshStore struct {
	deadline   time.Time
	inspectErr error
}

type uncertainUseStore struct {
	testStore
	failure error
}

func (s *uncertainUseStore) UseSession(context.Context, [32]byte, func() time.Time, time.Duration, *cognito.RefreshTokenProtector) (Principal, error) {
	return Principal{}, s.failure
}

func (*deadlineRefreshStore) ReserveCognitoRefresh(context.Context, cognito.RefreshReservationRequest) (cognito.RefreshReservation, error) {
	return cognito.RefreshReservation{}, errors.New("unexpected refresh reservation")
}

func (*deadlineRefreshStore) RecordCognitoRefreshFailure(context.Context, cognito.RefreshFailure) error {
	return errors.New("unexpected refresh failure audit")
}

func (*deadlineRefreshStore) CompleteCognitoRefresh(context.Context, cognito.RefreshCompletionRequest) error {
	return errors.New("unexpected refresh completion")
}

func (s *deadlineRefreshStore) InspectCognitoRefresh(ctx context.Context, _ [32]byte, _ time.Time) (CognitoRefreshState, error) {
	s.deadline, _ = ctx.Deadline()
	if s.inspectErr != nil {
		return 0, s.inspectErr
	}
	return 0, ErrInvalidSession
}

func TestCognitoRefreshInspectionFailurePreservesCause(t *testing.T) {
	rollbackFailure := errors.New("rollback failed")
	store := &deadlineRefreshStore{inspectErr: errors.Join(ErrInvalidSession, rollbackFailure)}
	manager := &Manager{
		now: time.Now,
		cognitoRefresh: &cognitoRefreshCoordinator{
			store: store,
		},
	}
	err := manager.runCognitoRefresh([32]byte{1})
	if err == ErrInvalidSession || !errors.Is(err, ErrCognitoRefreshUnavailable) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("inspection failure was mistaken for a definite invalid session: %v", err)
	}
}

func TestCognitoRefreshDoesNotStartAfterUncertainSessionUse(t *testing.T) {
	failure := errors.Join(ErrInvalidSession, errors.New("rollback failed"))
	inspection := &deadlineRefreshStore{}
	manager := &Manager{
		store:        &uncertainUseStore{failure: failure},
		now:          time.Now,
		idleLifetime: time.Minute,
		cognitoRefresh: &cognitoRefreshCoordinator{
			store: inspection, active: make(map[[32]byte]*cognitoRefreshCall),
		},
	}
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	_, err := manager.Resolve(context.Background(), token)
	if err != failure || !inspection.deadline.IsZero() {
		t.Fatal("uncertain session validation triggered refresh or lost its error")
	}
}

func TestCognitoRefreshCoordinatorBoundsWorkAndWaiters(t *testing.T) {
	if maxCognitoRefreshes != 32 || cognitoRefreshTimeout != 15*time.Second {
		t.Fatal("refresh coordination limits differ from the documented limits")
	}
	store := &deadlineRefreshStore{}
	coordinator := &cognitoRefreshCoordinator{store: store, active: make(map[[32]byte]*cognitoRefreshCall)}
	manager := &Manager{now: time.Now, cognitoRefresh: coordinator}
	for i := 1; i <= 32; i++ {
		var hash [32]byte
		hash[0] = byte(i)
		coordinator.active[hash] = &cognitoRefreshCall{done: make(chan struct{})}
	}
	var next [32]byte
	next[0] = 33
	if _, err := manager.resolveCognitoRefresh(context.Background(), next); !errors.Is(err, ErrCognitoRefreshUnavailable) ||
		len(coordinator.active) != 32 {
		t.Fatalf("concurrent refresh limit did not reject excess work: %v", err)
	}
	waiting, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.resolveCognitoRefresh(waiting, [32]byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("existing refresh waiter did not observe cancellation: %v", err)
	}
	delete(coordinator.active, [32]byte{32})
	started := time.Now()
	if _, err := manager.resolveCognitoRefresh(context.Background(), next); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("available refresh slot did not run inspection: %v", err)
	}
	if store.deadline.Before(started.Add(14*time.Second)) || store.deadline.After(started.Add(16*time.Second)) {
		t.Fatal("shared refresh did not use a bounded 15-second deadline")
	}
}
