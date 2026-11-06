package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func cognitoRefreshRequest(t *testing.T, credentials session.Credentials, protector *cognito.RefreshTokenProtector, now time.Time) cognito.RefreshReservationRequest {
	t.Helper()
	token, err := base64.RawURLEncoding.Strict().DecodeString(credentials.Token)
	if err != nil {
		t.Fatal(err)
	}
	operation := make([]byte, 16)
	if _, err := rand.Read(operation); err != nil {
		t.Fatal(err)
	}
	return cognito.RefreshReservationRequest{
		OperationID: base64.RawURLEncoding.EncodeToString(operation), TokenHash: sha256.Sum256(token),
		Protector: protector, OccurredAt: now,
	}
}

func TestCognitoRefreshReservationPersistsAndBlocksReplay(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	start, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	request := cognitoRefreshRequest(t, credentials, protector, f.clock())
	reservation, err := f.db.ReserveCognitoRefresh(ctx, request)
	if err != nil || reservation.OperationID != request.OperationID ||
		reservation.SessionID != credentials.Principal.SessionID || reservation.IdentityID != credentials.Principal.AccountID ||
		reservation.Generation != 1 || reservation.PreviousIdentity.Issuer != f.provider.Issuer ||
		reservation.PreviousIdentity.Subject != "opaque|synthetic-subject" ||
		!reflect.DeepEqual(reservation.PreviousIdentity.Audience, []string{f.provider.ClientID}) ||
		!reflect.DeepEqual(reservation.PreviousIdentity.Groups, []string{"unmapped-group"}) ||
		reservation.OriginalNonceHash != sha256.Sum256([]byte(start.Nonce)) ||
		!reservation.IdleExpiresAt.Equal(credentials.Principal.IdleExpiresAt) ||
		!reservation.AbsoluteExpiresAt.Equal(credentials.Principal.AbsoluteExpiresAt) ||
		bytes.Contains(reservation.EncryptedRefreshToken, []byte(fixtureRefresh)) {
		t.Fatalf("refresh reservation did not recover its protected snapshot: %v", err)
	}
	binding := cognito.RefreshTokenBinding{SessionID: reservation.SessionID, IdentityID: reservation.IdentityID, Generation: reservation.Generation}
	if token, err := protector.Open(binding, reservation.EncryptedRefreshToken); err != nil || token != fixtureRefresh {
		t.Fatalf("reserved refresh credential did not decrypt: %v", err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("unresolved refresh allowed session use: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted ||
		events[0].OperationID != request.OperationID || events[0].ActorID != credentials.Principal.AccountID ||
		events[0].ResourceID != credentials.Principal.SessionID {
		t.Fatalf("refresh reservation audit was incorrect: count=%d err=%v", len(events), err)
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, secret := range []string{fixtureRefresh, credentials.Token, credentials.CSRFToken, start.Nonce,
		"opaque|synthetic-subject", f.provider.Issuer, "unmapped-group"} {
		if strings.Contains(serialized, secret) {
			t.Fatal("refresh reservation audit contained a credential or provider claim")
		}
	}
	f.reopen(t)
	manager, protector = cognitoSessionTools(t, f)
	replay, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, credentials, protector, f.clock()))
	if !errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(replay, cognito.RefreshReservation{}) {
		t.Fatalf("refresh reservation was replayed after restart: %v", err)
	}
	if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("restart revived an unresolved refresh: %v", err)
	}
	for _, path := range []string{f.path, f.path + "-wal"} {
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		for _, secret := range []string{fixtureRefresh, credentials.Token, credentials.CSRFToken, start.Nonce} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("a provider or session credential was stored as plaintext")
			}
		}
	}
	var logs bytes.Buffer
	handler := cognitoSessionHTTPHandler(t, f, manager, &logs)
	logout := adminSessionsTestRequest(handler, credentials, http.MethodPost, "/api/session/logout")
	if logout.Code != http.StatusNoContent {
		t.Fatalf("unresolved refresh prevented local logout: status=%d", logout.Code)
	}
	var retained int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("local logout retained refresh credentials: count=%d err=%v", retained, err)
	}
}

func TestCognitoRefreshReservationSerializesConcurrentRequests(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	_, exchange := cognitoSessionExchange(t, f)
	manager, protector := cognitoSessionTools(t, f)
	credentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	const callers = 12
	requests := make([]cognito.RefreshReservationRequest, callers)
	for i := range requests {
		requests[i] = cognitoRefreshRequest(t, credentials, protector, f.clock())
	}
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for _, request := range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := f.db.ReserveCognitoRefresh(ctx, request)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	success, denied := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, cognito.ErrRefreshUnavailable):
			denied++
		default:
			t.Fatalf("unexpected concurrent refresh reservation error: %v", err)
		}
	}
	if success != 1 || denied != callers-1 || f.tokenCalls.Load() != 1 {
		t.Fatalf("refresh was not reserved once: success=%d denied=%d", success, denied)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 20})
	if err != nil || len(events) != callers {
		t.Fatalf("concurrent reservations were not audited: count=%d err=%v", len(events), err)
	}
}

func TestCognitoRefreshReservationAllowsStaleClaimsButNotUnavailableSessions(t *testing.T) {
	for _, tt := range []struct {
		name        string
		unavailable func(*testing.T, *cognitoCodeFixture, session.Credentials, *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector
		wantError   error
	}{
		{"stale claims", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			var deadline int64
			if err := f.db.db.QueryRowContext(context.Background(), "SELECT claims_fresh_until_ns FROM cognito_session_credentials WHERE session_id = ?", credentials.Principal.SessionID).Scan(&deadline); err != nil {
				t.Fatal(err)
			}
			f.current.Store(deadline)
			return protector
		}, nil},
		{"disabled identity", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE identities SET enabled = 0 WHERE id = ?", credentials.Principal.AccountID); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshUnavailable},
		{"idle expiration", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			f.current.Store(credentials.Principal.IdleExpiresAt.UnixNano())
			return protector
		}, cognito.ErrRefreshUnavailable},
		{"absolute expiration", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			f.current.Store(credentials.Principal.AbsoluteExpiresAt.UnixNano())
			return protector
		}, cognito.ErrRefreshUnavailable},
		{"revoked", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			manager, _ := cognitoSessionTools(t, f)
			if err := manager.Revoke(context.Background(), credentials.Token); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshUnavailable},
		{"wrong encryption key", func(t *testing.T, f *cognitoCodeFixture, _ session.Credentials, _ *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			protector, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{10}, 32))
			if err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshTokenProtection},
		{"wrong callback", func(t *testing.T, f *cognitoCodeFixture, _ session.Credentials, _ *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			attempts, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, "https://console.example/other-callback")
			if err != nil {
				t.Fatal(err)
			}
			protector, err := cognito.NewRefreshTokenProtector(attempts, rand.Reader, bytes.Repeat([]byte{9}, 32))
			if err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshTokenProtection},
		{"tampered ciphertext", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?", bytes.Repeat([]byte{1}, 32), credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshTokenProtection},
		{"missing original nonce", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE cognito_session_credentials SET original_nonce_hash = ? WHERE session_id = ?", make([]byte, 32), credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshState},
		{"invalid audience snapshot", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE cognito_session_credentials SET audience_json = 'null' WHERE session_id = ?", credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshState},
		{"invalid groups snapshot", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE cognito_session_credentials SET groups_json = 'null' WHERE session_id = ?", credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshState},
		{"generation exhausted", func(t *testing.T, f *cognitoCodeFixture, credentials session.Credentials, protector *cognito.RefreshTokenProtector) *cognito.RefreshTokenProtector {
			t.Helper()
			if _, err := f.db.db.ExecContext(context.Background(), "UPDATE cognito_session_credentials SET refresh_generation = ? WHERE session_id = ?", int64(math.MaxInt64), credentials.Principal.SessionID); err != nil {
				t.Fatal(err)
			}
			return protector
		}, cognito.ErrRefreshState},
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
			protector = tt.unavailable(t, f, credentials, protector)
			if tt.wantError == nil {
				if _, err := manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
					t.Fatalf("stale claims were usable before refresh: %v", err)
				}
			}
			reservation, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, credentials, protector, f.clock()))
			if tt.wantError == nil {
				if err != nil || reservation.Generation != 1 {
					t.Fatalf("valid session could not reserve refresh: %v", err)
				}
			} else if !errors.Is(err, tt.wantError) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
				t.Fatalf("unavailable session reserved refresh: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 1 || (events[0].Outcome == AuditAttempted) != (tt.wantError == nil) {
				t.Fatalf("refresh decision was not audited: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestCognitoRefreshReservationRechecksQueuedAndCommittedExpiry(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("after-commit=%t", afterCommit), func(t *testing.T) {
			f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			_, exchange := cognitoSessionExchange(t, f)
			manager, protector := cognitoSessionTools(t, f)
			credentials, err := manager.IssueCognito(ctx, exchange)
			if err != nil {
				t.Fatal(err)
			}
			request := cognitoRefreshRequest(t, credentials, protector, f.clock())
			if afterCommit {
				calls := 0
				f.db.now = func() time.Time {
					calls++
					if calls >= 3 {
						return credentials.Principal.IdleExpiresAt
					}
					return f.clock()
				}
				reservation, err := f.db.ReserveCognitoRefresh(ctx, request)
				if !errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
					t.Fatalf("refresh committed after session expiry returned material: %v", err)
				}
			} else {
				err := runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(credentials.Principal.IdleExpiresAt.UnixNano())
				}, func() error {
					_, err := f.db.ReserveCognitoRefresh(ctx, request)
					return err
				})
				if !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("queued expired session reserved a refresh: %v", err)
				}
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 1 || (events[0].Outcome == AuditAttempted) != afterCommit {
				t.Fatalf("expiration decision was not durable: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestCognitoRefreshReservationFailsClosedOnAuditAndCommitFailure(t *testing.T) {
	for _, tt := range []struct {
		name, outcome string
		commitFailure bool
	}{
		{"attempt audit", "attempted", false},
		{"denial audit", "denied", false},
		{"attempt commit", "attempted", true},
		{"denial commit", "denied", true},
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
			if tt.outcome == "denied" {
				protector, err = cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{10}, 32))
				if err != nil {
					t.Fatal(err)
				}
			}
			trigger := `CREATE TRIGGER reject_refresh BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = '` + tt.outcome + `'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
			want := cognito.ErrRefreshAudit
			if tt.commitFailure {
				trigger = `CREATE TABLE refresh_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER reject_refresh AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = '` + tt.outcome + `'
BEGIN INSERT INTO refresh_commit_failure (identity_id) VALUES ('missing-identity'); END;`
				want = cognito.ErrRefreshUnknown
			}
			if _, err := f.db.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			reservation, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, credentials, protector, f.clock()))
			if !errors.Is(err, want) || errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
				t.Fatalf("failed refresh reservation returned material: %v", err)
			}
			if f.tokenCalls.Load() != 1 {
				t.Fatal("failed reservation contacted the provider")
			}
		})
	}
}

func TestCognitoRefreshReservationDoesNotAffectLocalSessions(t *testing.T) {
	f := newCognitoCodeFixture(t, "success", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	manager, protector := cognitoSessionTools(t, f)
	_, exchange := cognitoSessionExchange(t, f)
	cognitoCredentials, err := manager.IssueCognito(ctx, exchange)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, cognitoCredentials, protector, f.clock())); err != nil {
		t.Fatal(err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "initial passphrase")
	if err != nil {
		t.Fatal(err)
	}
	admin := identity.LocalAccount{ID: "local-admin", Username: "admin", PasswordHash: hash, Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	if err := f.db.CreateInitialAdmin(ctx, admin, "bootstrap-event", "bootstrap-attempt", f.clock()); err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, credentials, protector, f.clock()))
	if !errors.Is(err, cognito.ErrRefreshUnavailable) || !reflect.DeepEqual(reservation, cognito.RefreshReservation{}) {
		t.Fatalf("local session was accepted for Cognito refresh: %v", err)
	}
	if principal, err := manager.Resolve(ctx, credentials.Token); err != nil || principal.Source != session.SourceLocal {
		t.Fatalf("Cognito refresh denial blocked local session: %v", err)
	}
}
