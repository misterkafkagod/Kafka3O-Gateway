package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundRefreshingProbeStore struct {
	*Database
	invalidUses chan struct{}
	successful  atomic.Int64
	completed   chan struct{}
	continueUse chan struct{}
}

func (s *boundRefreshingProbeStore) UseBoundSession(ctx context.Context, tokenHash, browserHash [32]byte, clock func() time.Time,
	idleLifetime time.Duration, protector *cognito.RefreshTokenProtector) (session.Principal, error) {
	principal, err := s.Database.UseBoundSession(ctx, tokenHash, browserHash, clock, idleLifetime, protector)
	if err == session.ErrInvalidSession && s.invalidUses != nil {
		s.invalidUses <- struct{}{}
	}
	if err == nil {
		s.successful.Add(1)
	}
	return principal, err
}

func (s *boundRefreshingProbeStore) CompleteBoundCognitoRefresh(ctx context.Context, request cognito.RefreshCompletionRequest, browserHash [32]byte) error {
	err := s.Database.CompleteBoundCognitoRefresh(ctx, request, browserHash)
	if err == nil && s.completed != nil {
		close(s.completed)
		select {
		case <-s.continueUse:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func boundRefreshingProbeManager(t *testing.T, f *boundRefreshingSessionFixture, store *boundRefreshingProbeStore) *session.Manager {
	t.Helper()
	attempts, err := cognito.NewAttemptManager(store, rand.Reader, f.clock, f.key, f.provider, f.callback)
	if err != nil {
		t.Fatal(err)
	}
	exchanger, err := cognito.NewCodeExchanger(attempts, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewRefreshingCognitoManager(store, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, exchanger, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestBoundCognitoSessionRefreshCoalescesOnlyMatchingWaiters(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	f.current.Store(f.freshUntil)
	const callers = 12
	store := &boundRefreshingProbeStore{Database: f.db, invalidUses: make(chan struct{}, callers+5)}
	manager := boundRefreshingProbeManager(t, f, store)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	f.mu.Lock()
	f.refreshEntered, f.refreshRelease = entered, release
	f.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			principal, err := manager.ResolveBound(context.Background(), f.credentials.Token, f.binding)
			if err == nil && (principal.SessionID != f.credentials.Principal.SessionID || principal.Source != session.SourceCognito) {
				err = errors.New("unexpected bound principal")
			}
			results <- err
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bound provider refresh did not start")
	}
	for i := 0; i < callers; i++ {
		select {
		case <-store.invalidUses:
		case <-time.After(5 * time.Second):
			t.Fatal("not all bound callers reached the pending refresh")
		}
	}
	wrong := sha256.Sum256([]byte("other in-flight browser"))
	wrongContext, cancelWrong := context.WithTimeout(context.Background(), time.Second)
	defer cancelWrong()
	if principal, err := manager.ResolveBound(wrongContext, f.credentials.Token, wrong); err != session.ErrInvalidSession || principal.SessionID != "" {
		t.Fatalf("different browser joined or learned a bound refresh: %v", err)
	}
	waiting, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := manager.ResolveBound(waiting, f.credentials.Token, f.binding); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bound waiter did not preserve its own cancellation: %v", err)
	}
	if f.refreshCalls.Load() != 1 {
		t.Fatal("bound waiters sent multiple provider requests")
	}
	close(release)
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("matching bound waiter failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("bound refresh waiter did not finish")
		}
	}
	if f.refreshCalls.Load() != 1 || store.successful.Load() != callers {
		t.Fatalf("each bound waiter did not revalidate independently: successful=%d requests=%d",
			store.successful.Load(), f.refreshCalls.Load())
	}
}

func TestBoundCognitoSessionRefreshSurvivesFirstCallerCancellation(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh no rotation")
	f.current.Store(f.freshUntil)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	f.mu.Lock()
	f.refreshEntered, f.refreshRelease = entered, release
	f.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := f.sessions.ResolveBound(ctx, f.credentials.Token, f.binding)
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bound provider refresh did not start")
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first bound caller did not preserve cancellation: %v", err)
	}
	waiter := make(chan error, 1)
	go func() {
		_, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding)
		waiter <- err
	}()
	close(release)
	if err := <-waiter; err != nil || f.refreshCalls.Load() != 1 {
		t.Fatalf("first caller cancellation interrupted shared bound work: %v", err)
	}
}

func TestBoundCognitoSessionRefreshConcurrentManagersDoNotReplay(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	f.current.Store(f.freshUntil)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	f.mu.Lock()
	f.refreshEntered, f.refreshRelease = entered, release
	f.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	first := make(chan error, 1)
	go func() {
		_, err := f.sessions.ResolveBound(context.Background(), f.credentials.Token, f.binding)
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bound provider refresh did not start")
	}
	other, err := session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, f.exchanger, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	if principal, err := other.ResolveBound(context.Background(), f.credentials.Token, f.binding); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" || f.refreshCalls.Load() != 1 {
		t.Fatalf("another manager replayed or invalidated pending bound work: %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first bound manager did not complete refresh: %v", err)
	}
	if principal, err := other.ResolveBound(context.Background(), f.credentials.Token, f.binding); err != nil ||
		principal.SessionID != f.credentials.Principal.SessionID || f.refreshCalls.Load() != 1 {
		t.Fatalf("completed bound refresh was not visible to the other manager: %v", err)
	}
}

func TestBoundCognitoSessionRefreshRevalidatesAfterCompletion(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	f.current.Store(f.freshUntil)
	store := &boundRefreshingProbeStore{
		Database: f.db, completed: make(chan struct{}), continueUse: make(chan struct{}),
	}

	manager := boundRefreshingProbeManager(t, f, store)
	t.Cleanup(func() {
		select {
		case <-store.continueUse:
		default:
			close(store.continueUse)
		}
	})
	type resolved struct {
		principal session.Principal
		err       error
	}
	done := make(chan resolved, 1)
	go func() {
		principal, err := manager.ResolveBound(context.Background(), f.credentials.Token, f.binding)
		done <- resolved{principal: principal, err: err}
	}()
	select {
	case <-store.completed:
	case <-time.After(5 * time.Second):
		t.Fatal("bound completion did not commit")
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	replacement, replacementToken := browserSessionRecord("after-bound-completion", "browser-local-b", f.binding, f.clock())
	if _, err := f.db.CreateLocalSession(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	close(store.continueUse)
	select {
	case result := <-done:
		if result.err != session.ErrInvalidSession || result.principal.SessionID != "" {
			t.Fatalf("bound waiter returned a session replaced after completion: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bound waiter did not finish revalidation")
	}
	if principal, err := manager.ResolveBound(context.Background(), replacementToken, f.binding); err != nil ||
		principal.SessionID != replacement.SessionID || f.refreshCalls.Load() != 1 {
		t.Fatalf("waiter revalidation affected the replacement: %v", err)
	}
}

func TestBoundCognitoSessionRefreshPreservesWaiterStorageFailure(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	f.current.Store(f.freshUntil)
	store := &boundRefreshingProbeStore{
		Database: f.db, completed: make(chan struct{}), continueUse: make(chan struct{}),
	}
	manager := boundRefreshingProbeManager(t, f, store)
	t.Cleanup(func() {
		select {
		case <-store.continueUse:
		default:
			close(store.continueUse)
		}
	})
	type resolved struct {
		principal session.Principal
		err       error
	}
	done := make(chan resolved, 1)
	go func() {
		principal, err := manager.ResolveBound(context.Background(), f.credentials.Token, f.binding)
		done <- resolved{principal: principal, err: err}
	}()
	select {
	case <-store.completed:
	case <-time.After(5 * time.Second):
		t.Fatal("bound completion did not commit")
	}
	if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
		t.Fatal(err)
	}
	close(store.continueUse)
	select {
	case result := <-done:
		if result.err == nil || errors.Is(result.err, session.ErrInvalidSession) || result.principal.SessionID != "" || f.refreshCalls.Load() != 1 {
			t.Fatalf("waiter hid an operational binding failure: %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bound waiter did not report binding storage failure")
	}
}
