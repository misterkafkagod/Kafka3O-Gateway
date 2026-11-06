package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type refreshingSessionFixture struct {
	*cognitoCodeFixture
	sessions    *session.Manager
	protector   *cognito.RefreshTokenProtector
	credentials session.Credentials
	freshUntil  int64
}

func newRefreshingSessionFixture(t *testing.T, mode string) *refreshingSessionFixture {
	t.Helper()
	f := &refreshingSessionFixture{cognitoCodeFixture: newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)}
	_, exchange := cognitoSessionExchange(t, f.cognitoCodeFixture)
	_, f.protector = cognitoSessionTools(t, f.cognitoCodeFixture)
	var err error
	f.sessions, err = session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, f.exchanger, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	f.credentials, err = f.sessions.IssueCognito(context.Background(), exchange)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.db.QueryRow("SELECT claims_fresh_until_ns FROM cognito_session_credentials WHERE session_id = ?",
		f.credentials.Principal.SessionID).Scan(&f.freshUntil); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
	return f
}

func (f *refreshingSessionFixture) makeStale() {
	f.current.Store(f.freshUntil)
}

func (f *refreshingSessionFixture) pauseProvider(t *testing.T) (<-chan struct{}, chan struct{}) {
	t.Helper()
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
	return entered, release
}

func TestCognitoSessionRefreshRevalidatesAndPersistsRotation(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	if principal, err := f.sessions.Resolve(ctx, f.credentials.Token); err != nil || principal.Source != session.SourceCognito ||
		f.refreshCalls.Load() != 0 {
		t.Fatalf("fresh session unexpectedly refreshed: %v", err)
	}
	f.makeStale()
	principal, err := f.sessions.Resolve(ctx, f.credentials.Token)
	if err != nil || principal.AccountID != f.credentials.Principal.AccountID || principal.Source != session.SourceCognito ||
		f.refreshCalls.Load() != 1 || f.invalidCalls.Load() != 0 {
		t.Fatalf("stale session was not refreshed and revalidated: %v", err)
	}
	var generation int64
	var groups string
	var encrypted []byte
	if err := f.db.db.QueryRowContext(ctx, `SELECT refresh_generation, groups_json, encrypted_refresh_token
FROM cognito_session_credentials WHERE session_id = ?`, principal.SessionID).Scan(&generation, &groups, &encrypted); err != nil {
		t.Fatal(err)
	}
	binding := cognito.RefreshTokenBinding{SessionID: principal.SessionID, IdentityID: principal.AccountID, Generation: 2}
	if token, err := f.protector.Open(binding, encrypted); err != nil || token != fixtureRotated || generation != 2 || groups != `["refreshed-group"]` {
		t.Fatalf("session refresh did not commit the verified next generation: %v", err)
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); err != nil || f.refreshCalls.Load() != 1 {
		t.Fatalf("freshly resolved session repeated a provider request: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded {
		t.Fatalf("session refresh did not durably audit success: count=%d err=%v", len(events), err)
	}
}

func TestCognitoSessionRefreshCoalescesConcurrentRequests(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	entered, release := f.pauseProvider(t)
	const callers = 12
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for i := 0; i < callers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			principal, err := f.sessions.Resolve(context.Background(), f.credentials.Token)
			if err == nil && (principal.AccountID != f.credentials.Principal.AccountID || principal.Source != session.SourceCognito) {
				err = errors.New("unexpected principal")
			}
			results <- err
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not start")
	}
	cancelled, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := f.sessions.Resolve(cancelled, f.credentials.Token); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting caller did not preserve its own cancellation: %v", err)
	}
	if f.refreshCalls.Load() != 1 {
		t.Fatalf("concurrent sessions sent multiple refresh requests: %d", f.refreshCalls.Load())
	}
	close(release)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("coalesced session request failed: %v", err)
		}
	}
	if f.refreshCalls.Load() != 1 {
		t.Fatal("concurrent waiters retried the provider")
	}
}

func TestCognitoSessionRefreshSurvivesFirstCallerCancellation(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh no rotation")
	f.makeStale()
	entered, release := f.pauseProvider(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := f.sessions.Resolve(ctx, f.credentials.Token)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled first caller was not released: %v", err)
	}
	waiter := make(chan error, 1)
	go func() {
		_, err := f.sessions.Resolve(context.Background(), f.credentials.Token)
		waiter <- err
	}()
	close(release)
	if err := <-waiter; err != nil || f.refreshCalls.Load() != 1 {
		t.Fatalf("first caller cancellation interrupted shared refresh: %v", err)
	}
}

func TestCognitoSessionRefreshConcurrentManagersKeepCookies(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	entered, release := f.pauseProvider(t)
	first := make(chan error, 1)
	go func() {
		_, err := f.sessions.Resolve(context.Background(), f.credentials.Token)
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not start")
	}
	other, err := session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, f.exchanger, f.protector)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f.cognitoCodeFixture, other, &logs)
	response := adminSessionsTestRequest(handler, f.credentials, http.MethodGet, "/api/session")
	if response.Code != http.StatusServiceUnavailable || len(response.Result().Cookies()) != 0 {
		t.Fatalf("in-flight refresh cleared cookies or returned a false 401: status=%d", response.Code)
	}
	if f.refreshCalls.Load() != 1 {
		t.Fatal("another manager sent a concurrent provider refresh")
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first manager did not complete refresh: %v", err)
	}
	response = adminSessionsTestRequest(handler, f.credentials, http.MethodGet, "/api/session")
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 0 {
		t.Fatalf("completed refresh was not visible to the other manager: status=%d", response.Code)
	}
	for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, f.credentials.Token, f.credentials.CSRFToken} {
		if bytes.Contains(logs.Bytes(), []byte(secret)) || bytes.Contains(response.Body.Bytes(), []byte(secret)) {
			t.Fatal("concurrent refresh exposed a credential")
		}
	}
}

func TestCognitoSessionRefreshOutageKeepsLogoutAndLocalLoginAvailable(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh outage")
	f.makeStale()
	ctx := context.Background()
	if principal, err := f.sessions.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		!errors.Is(err, cognito.ErrRefreshUnknown) || errors.Is(err, session.ErrInvalidSession) || principal.SessionID != "" {
		t.Fatalf("provider outage granted access or produced a false 401: %v", err)
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		errors.Is(err, session.ErrInvalidSession) || f.refreshCalls.Load() != 1 {
		t.Fatalf("provider outage retried or cleared an unresolved refresh: %v", err)
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f.cognitoCodeFixture, f.sessions, &logs)
	response := adminSessionsTestRequest(handler, f.credentials, http.MethodGet, "/api/session")
	if response.Code != http.StatusServiceUnavailable || len(response.Result().Cookies()) != 0 {
		t.Fatalf("unresolved refresh cleared cookies or returned a false 401: status=%d", response.Code)
	}
	for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, f.credentials.Token,
		f.credentials.CSRFToken, "opaque|synthetic-subject", "unmapped-group"} {
		if bytes.Contains(logs.Bytes(), []byte(secret)) || bytes.Contains(response.Body.Bytes(), []byte(secret)) {
			t.Fatal("refresh outage exposed provider or session material")
		}
	}
	passwordHash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: passwordHash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	login, err := session.NewLocalLogin(authenticator, f.sessions)
	if err != nil {
		t.Fatal(err)
	}
	local, err := login.Execute(ctx, "admin", "initial passphrase", netip.MustParseAddr("127.0.0.1"))
	if err != nil || local.Principal.Source != session.SourceLocal {
		t.Fatalf("provider outage blocked local login: %v", err)
	}
	if _, err := f.sessions.Resolve(ctx, local.Token); err != nil {
		t.Fatalf("provider outage blocked local session: %v", err)
	}
	logout := adminSessionsTestRequest(handler, f.credentials, http.MethodPost, "/api/session/logout")
	if logout.Code != http.StatusNoContent || len(logout.Result().Cookies()) != 2 {
		t.Fatalf("provider outage blocked local Cognito logout: status=%d", logout.Code)
	}
	var retained int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?",
		f.credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("logout retained the unresolved refresh credential: count=%d err=%v", retained, err)
	}
}

func TestCognitoSessionRefreshLogoutWinsDuringProviderRequest(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	entered, release := f.pauseProvider(t)
	first := make(chan error, 1)
	go func() {
		_, err := f.sessions.Resolve(context.Background(), f.credentials.Token)
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not start")
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f.cognitoCodeFixture, f.sessions, &logs)
	response := adminSessionsTestRequest(handler, f.credentials, http.MethodPost, "/api/session/logout")
	if response.Code != http.StatusNoContent || len(response.Result().Cookies()) != 2 {
		t.Fatalf("in-flight refresh blocked CSRF-checked logout: status=%d", response.Code)
	}
	close(release)
	if err := <-first; !errors.Is(err, session.ErrCognitoRefreshUnavailable) {
		t.Fatalf("in-flight refresh revived a logged-out session: %v", err)
	}
	if _, err := f.sessions.Resolve(context.Background(), f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("revoked session remained usable: %v", err)
	}
	var retained int
	if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("logout retained a refresh credential: count=%d err=%v", retained, err)
	}
}

func TestCognitoSessionRefreshAuditFailureDoesNotRetryProvider(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	ctx := context.Background()
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_refresh_success BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'succeeded'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		!errors.Is(err, cognito.ErrRefreshAudit) || errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("refresh audit failure did not fail closed: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_refresh_success"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		f.refreshCalls.Load() != 1 {
		t.Fatalf("failed refresh was automatically retried: %v", err)
	}
}

func TestCognitoSessionRefreshRestartDoesNotReplayReservation(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	ctx := context.Background()
	request := cognitoRefreshRequest(t, f.credentials, f.protector, f.clock())
	if _, err := f.db.ReserveCognitoRefresh(ctx, request); err != nil {
		t.Fatal(err)
	}
	oldExchanger := f.exchanger
	f.reopen(t)
	_, protector := cognitoSessionTools(t, f.cognitoCodeFixture)
	if _, err := session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, oldExchanger, protector); !errors.Is(err, session.ErrInvalidSessionConfig) {
		t.Fatalf("refresh manager accepted an exchanger attached to a different store: %v", err)
	}
	var err error
	f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
	if err != nil {
		t.Fatal(err)
	}
	f.sessions, err = session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime,
		session.DefaultAbsoluteLifetime, f.exchanger, protector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.sessions.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrCognitoRefreshUnavailable) ||
		errors.Is(err, session.ErrInvalidSession) || f.refreshCalls.Load() != 0 {
		t.Fatalf("restart replayed or cleared an unresolved refresh: %v", err)
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f.cognitoCodeFixture, f.sessions, &logs)
	response := adminSessionsTestRequest(handler, f.credentials, http.MethodGet, "/api/session")
	if response.Code != http.StatusServiceUnavailable || len(response.Result().Cookies()) != 0 {
		t.Fatalf("restart turned unresolved refresh into logout: status=%d", response.Code)
	}
}

func TestCognitoSessionRefreshInactiveAndInvalidSessionsDoNotDispatch(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*testing.T, *refreshingSessionFixture)
	}{
		{"disabled", func(t *testing.T, f *refreshingSessionFixture) {
			_, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID)
			if err != nil {
				t.Fatal(err)
			}
		}},
		{"revoked", func(t *testing.T, f *refreshingSessionFixture) {
			if err := f.sessions.Revoke(context.Background(), f.credentials.Token); err != nil {
				t.Fatal(err)
			}
		}},
		{"expired", func(t *testing.T, f *refreshingSessionFixture) {
			f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newRefreshingSessionFixture(t, "refresh rotated")
			f.makeStale()
			tt.change(t, f)
			if _, err := f.sessions.Resolve(context.Background(), f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) ||
				f.refreshCalls.Load() != 0 {
				t.Fatalf("inactive session dispatched a provider refresh: %v", err)
			}
		})
	}
}

func TestCognitoSessionRefreshStatusAtExactDeadline(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh no rotation")
	request := cognitoRefreshRequest(t, f.credentials, f.protector, f.clock())
	f.current.Store(f.freshUntil - 1)
	state, err := f.db.InspectCognitoRefresh(context.Background(), request.TokenHash, f.clock())
	if err != nil || state != session.CognitoRefreshFresh {
		t.Fatalf("claims became stale before the exact deadline: state=%d err=%v", state, err)
	}
	f.current.Store(f.freshUntil)
	state, err = f.db.InspectCognitoRefresh(context.Background(), request.TokenHash, f.clock())
	if err != nil || state != session.CognitoRefreshReady {
		t.Fatalf("claims remained fresh at the exact deadline: state=%d err=%v", state, err)
	}
	request.OccurredAt = f.clock()
	if _, err := f.db.ReserveCognitoRefresh(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	state, err = f.db.InspectCognitoRefresh(context.Background(), request.TokenHash, f.clock())
	if err != nil || state != session.CognitoRefreshBlocked {
		t.Fatalf("reserved generation was not blocked: state=%d err=%v", state, err)
	}
}

func TestCognitoSessionRefreshInvalidCredentialStatePreservesCookie(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	f.makeStale()
	if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f.cognitoCodeFixture, f.sessions, &logs)
	response := adminSessionsTestRequest(handler, f.credentials, http.MethodGet, "/api/session")
	if response.Code != http.StatusServiceUnavailable || len(response.Result().Cookies()) != 0 || f.refreshCalls.Load() != 0 {
		t.Fatalf("missing credentials produced a false logout: status=%d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "session_service_unavailable") || strings.Contains(fmt.Sprint(logs.String()), f.credentials.Token) {
		t.Fatal("invalid credential state was not reported safely")
	}
}

func TestCognitoSessionRefreshHTTPWaitCancellationKeepsCookies(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh no rotation")
	f.makeStale()
	entered, release := f.pauseProvider(t)
	first := make(chan error, 1)
	go func() {
		_, err := f.sessions.Resolve(context.Background(), f.credentials.Token)
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider refresh did not start")
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f.cognitoCodeFixture, f.sessions, &logs)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "https://console.example/api/session", nil).WithContext(ctx)
	request.AddCookie(&http.Cookie{Name: session.CookieName, Value: f.credentials.Token})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || len(response.Result().Cookies()) != 0 {
		t.Fatalf("canceled refresh waiter cleared cookies: status=%d", response.Code)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("canceled waiter interrupted shared refresh: %v", err)
	}
}
