package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func cognitoSessionHTTPHandler(t *testing.T, f *cognitoCodeFixture, manager *session.Manager, logs *bytes.Buffer) http.Handler {
	t.Helper()
	change, err := identity.NewSetLocalAccountEnabled(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	return adminSessionsTestHandler(t, accountAdministrationFixture{db: f.db, manager: manager, change: change}, list, revoke, logs)
}

func TestCognitoSessionRequiresConfiguredProviderAndProtection(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	localOnly, err := session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localOnly.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("local-only manager accepted a Cognito session: %v", err)
	}
	if _, err := localOnly.IssueCognito(ctx, exchange); !errors.Is(err, session.ErrInvalidSessionConfig) {
		t.Fatalf("local-only manager issued a Cognito session: %v", err)
	}
	if _, err := session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, nil, protector); !errors.Is(err, session.ErrInvalidSessionConfig) {
		t.Fatalf("manager accepted a missing verified provider: %v", err)
	}
	if _, err := session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, nil); !errors.Is(err, session.ErrInvalidSessionConfig) {
		t.Fatalf("manager accepted a missing protector: %v", err)
	}
	wrongKey, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{10}, 32))
	if err != nil {
		t.Fatal(err)
	}
	wrongKeyManager, err := session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, wrongKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongKeyManager.Resolve(ctx, credentials.Token); !errors.Is(err, cognito.ErrRefreshTokenProtection) {
		t.Fatalf("session survived a refresh-key change: %v", err)
	}
	otherCallback := "https://console.example/other-callback"
	otherAttempts, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, otherCallback)
	if err != nil {
		t.Fatal(err)
	}
	otherProtector, err := cognito.NewRefreshTokenProtector(otherAttempts, rand.Reader, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, otherProtector); !errors.Is(err, session.ErrInvalidSessionConfig) {
		t.Fatalf("manager accepted a mismatched provider and protector: %v", err)
	}
	otherExchanger, err := cognito.NewCodeExchanger(otherAttempts, f.verifier, otherCallback, cognito.ClientAuthenticationPublic, "")
	if err != nil {
		t.Fatal(err)
	}
	otherManager, err := session.NewCognitoManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, otherExchanger, otherProtector)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherManager.Resolve(ctx, credentials.Token); !errors.Is(err, cognito.ErrRefreshTokenProtection) {
		t.Fatalf("session survived a callback change: %v", err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	localCredentials, err := localOnly.IssueLocal(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range []*session.Manager{localOnly, wrongKeyManager, otherManager} {
		principal, err := current.Resolve(ctx, localCredentials.Token)
		if err != nil || principal.Source != session.SourceLocal {
			t.Fatalf("Cognito configuration blocked local login: %v", err)
		}
	}
	var encrypted []byte
	if err := f.db.db.QueryRowContext(ctx, "SELECT encrypted_refresh_token FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	encrypted[len(encrypted)-1] ^= 1
	if _, err := f.db.db.ExecContext(ctx, "UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?", encrypted, credentials.Principal.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, cognito.ErrRefreshTokenProtection) {
		t.Fatalf("session accepted tampered refresh credentials: %v", err)
	}
}

func TestCognitoSessionRechecksFreshnessDuringValidation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		expireCall  int
		wantSliding bool
	}{
		{"before commit", 3, false},
		{"after commit", 4, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			_, exchange := cognitoSessionExchange(t, f)
			manager, protector := cognitoSessionTools(t, f)
			credentials, err := manager.IssueCognito(ctx, exchange)
			if err != nil {
				t.Fatal(err)
			}
			var freshNS, initialIdle int64
			if err := f.db.db.QueryRowContext(ctx, `SELECT cognito_session_credentials.claims_fresh_until_ns,
console_sessions.idle_expires_at_ns FROM cognito_session_credentials
JOIN console_sessions ON console_sessions.id = cognito_session_credentials.session_id
WHERE console_sessions.id = ?`, credentials.Principal.SessionID).Scan(&freshNS, &initialIdle); err != nil {
				t.Fatal(err)
			}
			calls := 0
			clock := func() time.Time {
				calls++
				if calls >= tt.expireCall {
					return time.Unix(0, freshNS)
				}
				return f.clock().Add(time.Minute)
			}
			queued, err := session.NewCognitoManager(f.db, rand.Reader, clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, protector)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := queued.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("session remained usable after claims expired during validation: %v", err)
			}
			var finalIdle int64
			if err := f.db.db.QueryRowContext(ctx, "SELECT idle_expires_at_ns FROM console_sessions WHERE id = ?", credentials.Principal.SessionID).Scan(&finalIdle); err != nil {
				t.Fatal(err)
			}
			if (finalIdle > initialIdle) != tt.wantSliding {
				t.Fatalf("unexpected idle update after expiry: initial=%d final=%d", initialIdle, finalIdle)
			}
		})
	}
}

func TestCognitoHTTPLogoutRevokesUnavailableSessions(t *testing.T) {
	for _, tt := range []struct {
		name            string
		wantInspection  int
		makeUnavailable func(*testing.T, *cognitoCodeFixture, session.Credentials, *session.Manager) *session.Manager
	}{
		{"stale claims", http.StatusUnauthorized, func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, manager *session.Manager) *session.Manager {
			t.Helper()
			var deadline int64
			if err := f.db.db.QueryRowContext(context.Background(), "SELECT claims_fresh_until_ns FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&deadline); err != nil {
				t.Fatal(err)
			}
			f.current.Store(deadline)
			return manager
		}},
		{"disabled identity", http.StatusUnauthorized, func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, manager *session.Manager) *session.Manager {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE identities SET enabled = 0 WHERE id = ?", credentials.Principal.AccountID); err != nil {
				t.Fatal(err)
			}
			return manager
		}},
		{"expired session", http.StatusUnauthorized, func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, manager *session.Manager) *session.Manager {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE console_sessions SET idle_expires_at_ns = created_at_ns + 1 WHERE id = ?", credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			f.current.Add(2)
			return manager
		}},
		{"provider disabled", http.StatusUnauthorized, func(t *testing.T, f *cognitoCodeFixture, _ session.Credentials, _ *session.Manager) *session.Manager {
			t.Helper()
			localOnly, err := session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
			if err != nil {
				t.Fatal(err)
			}
			return localOnly
		}},
		{"tampered ciphertext", http.StatusServiceUnavailable, func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, manager *session.Manager) *session.Manager {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?", bytes.Repeat([]byte{1}, 32), credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			return manager
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			_, exchange := cognitoSessionExchange(t, f)
			manager, _ := cognitoSessionTools(t, f)
			credentials, err := manager.IssueCognito(ctx, exchange)
			if err != nil {
				t.Fatal(err)
			}
			manager = tt.makeUnavailable(t, f, credentials, manager)
			var logs bytes.Buffer
			handler := cognitoSessionHTTPHandler(t, f, manager, &logs)
			inspect := adminSessionsTestRequest(handler, credentials, http.MethodGet, "/api/session")
			if inspect.Code != tt.wantInspection {
				t.Fatalf("unavailable session was usable: status=%d", inspect.Code)
			}
			wrongCSRF := credentials
			wrongCSRF.CSRFToken = "wrong-token"
			denied := adminSessionsTestRequest(handler, wrongCSRF, http.MethodPost, "/api/session/logout")
			if denied.Code != http.StatusForbidden {
				t.Fatalf("logout without a valid CSRF token status=%d", denied.Code)
			}
			var retained int
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 1 {
				t.Fatalf("denied logout removed credentials: count=%d err=%v", retained, err)
			}
			logout := adminSessionsTestRequest(handler, credentials, http.MethodPost, "/api/session/logout")
			if logout.Code != http.StatusNoContent {
				t.Fatalf("local logout of unavailable session status=%d", logout.Code)
			}
			cleared := logout.Result().Cookies()
			if len(cleared) != 2 || cleared[0].MaxAge >= 0 || cleared[1].MaxAge >= 0 {
				t.Fatal("local logout did not clear both application cookies")
			}
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
				t.Fatalf("logout retained provider credentials: count=%d err=%v", retained, err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.session.logout", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Source != AuditSourceCognito || events[0].ActorID != credentials.Principal.AccountID {
				t.Fatalf("logout was not audited: count=%d err=%v", len(events), err)
			}
			if f.tokenCalls.Load() != 1 {
				t.Fatal("local logout contacted the provider")
			}
			for _, secret := range []string{fixtureRefresh, credentials.Token, credentials.CSRFToken} {
				if bytes.Contains(logs.Bytes(), []byte(secret)) || bytes.Contains(inspect.Body.Bytes(), []byte(secret)) {
					t.Fatal("unavailable session exposed credentials")
				}
			}
		})
	}
}

func TestCognitoHTTPLogoutReportsAuditFailure(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, _ := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	var deadline int64
	if err := f.db.db.QueryRowContext(ctx, "SELECT claims_fresh_until_ns FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	f.current.Store(deadline)
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_cognito_http_logout BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.session.logout'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f, manager, &logs)
	response := adminSessionsTestRequest(handler, credentials, http.MethodPost, "/api/session/logout")
	if response.Code != http.StatusServiceUnavailable || len(response.Result().Cookies()) != 0 {
		t.Fatalf("failed logout was reported as successful or cleared cookies: status=%d", response.Code)
	}
	var retained int
	if err := f.db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM console_sessions
JOIN cognito_session_credentials ON cognito_session_credentials.session_id = console_sessions.id
WHERE console_sessions.id = ? AND console_sessions.revoked_at_ns IS NULL`, credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("failed logout partially revoked the session: count=%d err=%v", retained, err)
	}
	for _, secret := range []string{fixtureRefresh, credentials.Token, credentials.CSRFToken} {
		if bytes.Contains(logs.Bytes(), []byte(secret)) || bytes.Contains(response.Body.Bytes(), []byte(secret)) {
			t.Fatal("logout failure exposed credentials")
		}
	}
}
