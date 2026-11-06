package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type storedCognitoRefresh struct {
	generation, issued, expiry, freshUntil int64
	audience, groups                       string
	nonce, encrypted                       []byte
}

func readStoredCognitoRefresh(t *testing.T, f *cognitoCompletionFixture) (storedCognitoRefresh, bool) {
	t.Helper()
	var stored storedCognitoRefresh
	err := f.db.db.QueryRowContext(context.Background(), `SELECT refresh_generation, issued_at_ns,
token_expires_at_ns, claims_fresh_until_ns, audience_json, groups_json, original_nonce_hash, encrypted_refresh_token
FROM cognito_session_credentials WHERE session_id = ?`, f.details.SessionID).Scan(
		&stored.generation, &stored.issued, &stored.expiry, &stored.freshUntil,
		&stored.audience, &stored.groups, &stored.nonce, &stored.encrypted,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storedCognitoRefresh{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return stored, true
}

func TestCognitoRefreshCompletionRejectsChangedReservationAndSession(t *testing.T) {
	for _, tt := range []struct {
		name        string
		change      func(*testing.T, *cognitoCompletionFixture)
		want        error
		wantOutcome AuditOutcome
		missing     bool
	}{
		{"disabled identity", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.details.IdentityID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"revoked session", func(t *testing.T, f *cognitoCompletionFixture) {
			if err := f.manager.Revoke(context.Background(), f.credentials.Token); err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"idle expiration", func(t *testing.T, f *cognitoCompletionFixture) {
			f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"absolute expiration", func(t *testing.T, f *cognitoCompletionFixture) {
			f.current.Store(f.credentials.Principal.AbsoluteExpiresAt.UnixNano())
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"shortened idle deadline", func(t *testing.T, f *cognitoCompletionFixture) {
			deadline := f.clock().Add(time.Nanosecond)
			_, err := f.db.db.Exec("UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?", deadline.UnixNano(), f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			f.current.Store(deadline.UnixNano())
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"shortened absolute deadline", func(t *testing.T, f *cognitoCompletionFixture) {
			deadline := f.clock().Add(time.Nanosecond)
			_, err := f.db.db.Exec("UPDATE console_sessions SET idle_expires_at_ns = ?, absolute_expires_at_ns = ? WHERE id = ?",
				deadline.UnixNano(), deadline.UnixNano(), f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			f.current.Store(deadline.UnixNano())
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"authentication source", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE console_sessions SET auth_source = 'local' WHERE id = ?", f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"credential version", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE console_sessions SET credential_version = 2 WHERE id = ?", f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshUnavailable, AuditFailed, false},
		{"generation", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET refresh_generation = 2 WHERE session_id = ?", f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"nonce", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET original_nonce_hash = ? WHERE session_id = ?", bytes.Repeat([]byte{1}, 32), f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"audience", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec(`UPDATE cognito_session_credentials SET audience_json = '["changed"]' WHERE session_id = ?`, f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"groups", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec(`UPDATE cognito_session_credentials SET groups_json = '["changed"]' WHERE session_id = ?`, f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"issuance", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET issued_at_ns = issued_at_ns - 1 WHERE session_id = ?", f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"token expiry", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET token_expires_at_ns = token_expires_at_ns + 1 WHERE session_id = ?", f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"freshness deadline", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET claims_fresh_until_ns = claims_fresh_until_ns - 1 WHERE session_id = ?", f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"encrypted credential", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?", bytes.Repeat([]byte{1}, 32), f.details.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"issuer", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_identities SET issuer = 'https://other.example' WHERE identity_id = ?", f.details.IdentityID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"subject", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("UPDATE cognito_identities SET subject = 'changed' WHERE identity_id = ?", f.details.IdentityID)
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshState, AuditFailed, false},
		{"prior failure", func(t *testing.T, f *cognitoCompletionFixture) {
			if err := f.db.RecordCognitoRefreshFailure(context.Background(), cognito.RefreshFailure{
				OperationID: f.result.OperationID, SessionID: f.details.SessionID, Generation: f.details.PreviousGeneration,
				OccurredAt: f.clock(), Status: cognito.RefreshOutcomeUnknown,
			}); err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshUnavailable, AuditUnknown, false},
		{"missing reservation", func(t *testing.T, f *cognitoCompletionFixture) {
			_, err := f.db.db.Exec("DELETE FROM audit_events WHERE event_id = ?", cognitoRefreshEventID(f.details.SessionID, f.details.PreviousGeneration))
			if err != nil {
				t.Fatal(err)
			}
		}, cognito.ErrRefreshUnavailable, "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
			request := f.request()
			tt.change(t, f)
			before, existed := readStoredCognitoRefresh(t, f)
			if err := f.db.CompleteCognitoRefresh(context.Background(), request); !errors.Is(err, tt.want) {
				t.Fatalf("changed refresh state was accepted: %v", err)
			}
			after, stillExists := readStoredCognitoRefresh(t, f)
			if existed != stillExists || !reflect.DeepEqual(before, after) {
				t.Fatal("rejected completion changed stored credentials")
			}
			if err := f.db.CompleteCognitoRefresh(context.Background(), request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("rejected completion proof was reused: %v", err)
			}
			events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || (tt.missing && len(events) != 0) || (!tt.missing && (len(events) != 2 || events[1].Outcome != tt.wantOutcome)) {
				t.Fatalf("rejected completion audit was incorrect: count=%d err=%v", len(events), err)
			}
			for _, secret := range []string{fixtureRefresh, fixtureRotated, fixtureAccess, f.credentials.Token,
				"opaque|synthetic-subject", "refreshed-group"} {
				if strings.Contains(fmt.Sprintf("%+v", events), secret) {
					t.Fatal("completion audit exposed provider or session material")
				}
			}
			if !tt.missing {
				if _, err := f.manager.Resolve(context.Background(), f.credentials.Token); err == nil {
					t.Fatal("rejected completion restored the Cognito session")
				}
			}
		})
	}
}

func TestCognitoRefreshCompletionRequiresMatchingProofAndProtector(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	if err := f.db.CompleteCognitoRefresh(ctx, cognito.RefreshCompletionRequest{
		Protector: f.protector, OccurredAt: f.clock(),
	}); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("forged completion proof was accepted: %v", err)
	}
	wrongKey, err := cognito.NewRefreshTokenProtector(f.cognitoCodeFixture.manager, rand.Reader, bytes.Repeat([]byte{10}, 32))
	if err != nil {
		t.Fatal(err)
	}
	request := f.request()
	request.Protector = wrongKey
	if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshTokenProtection) {
		t.Fatalf("wrong completion key was accepted: %v", err)
	}
	otherAttempts, err := cognito.NewAttemptManager(f.db, rand.Reader, f.clock, f.key, f.provider, "https://console.example/other")
	if err != nil {
		t.Fatal(err)
	}
	otherProvider, err := cognito.NewRefreshTokenProtector(otherAttempts, rand.Reader, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	request.Protector = otherProvider
	if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("wrong provider binding was accepted: %v", err)
	}
	if err := f.db.CompleteCognitoRefresh(ctx, f.request()); err != nil {
		t.Fatalf("failed forged checks consumed the valid proof: %v", err)
	}
}

func TestCognitoRefreshCompletionAuditAndCommitFailuresBlockRetry(t *testing.T) {
	for _, commitFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%t", commitFailure), func(t *testing.T) {
			f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			before, _ := readStoredCognitoRefresh(t, f)
			trigger := `CREATE TRIGGER reject_refresh_success BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'succeeded'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`
			want := cognito.ErrRefreshAudit
			if commitFailure {
				trigger = `CREATE TABLE refresh_success_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER reject_refresh_success AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'succeeded'
BEGIN INSERT INTO refresh_success_commit_failure (identity_id) VALUES ('missing-identity'); END;`
				want = cognito.ErrRefreshUnknown
			}
			if _, err := f.db.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			request := f.request()
			if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, want) {
				t.Fatalf("refresh completion hid an audit or commit failure: %v", err)
			}
			after, _ := readStoredCognitoRefresh(t, f)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("audit or commit failure changed protected credentials")
			}
			if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_refresh_success"); err != nil {
				t.Fatal(err)
			}
			if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("failed completion proof was retried: %v", err)
			}
			anotherProof, err := f.result.ForCompletion(f.protector, f.clock())
			if err != nil {
				t.Fatal(err)
			}
			if err := f.db.CompleteCognitoRefresh(ctx, cognito.RefreshCompletionRequest{
				Refresh: anotherProof, Protector: f.protector, OccurredAt: f.clock(),
			}); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("a copied completion proof bypassed one-shot consumption: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("failed completion restored session access: %v", err)
			}
			hash := cognitoRefreshRequest(t, f.credentials, f.protector, f.clock()).TokenHash
			if _, err := f.exchanger.RefreshCognitoSession(ctx, hash, f.protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
				f.refreshCalls.Load() != 1 {
				t.Fatalf("failed completion allowed another provider request: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditDenied {
				t.Fatalf("failed completion lost its durable reservation: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestCognitoRefreshCompletionDenialAuditFailureBlocksRetry(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 0 WHERE id = ?", f.details.IdentityID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_refresh_denial BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'failed'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	request := f.request()
	if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshAudit) ||
		!errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("completion denial hid an audit failure: %v", err)
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_refresh_denial"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE identities SET enabled = 1 WHERE id = ?", f.details.IdentityID); err != nil {
		t.Fatal(err)
	}
	if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("failed denial audit allowed proof retry: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("failed denial audit restored session access: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
		t.Fatalf("failed denial audit lost its durable reservation: count=%d err=%v", len(events), err)
	}
}

func TestCognitoRefreshCompletionUpdateFailureBlocksRetry(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	before, _ := readStoredCognitoRefresh(t, f)
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_refresh_update BEFORE UPDATE ON cognito_session_credentials
BEGIN SELECT RAISE(FAIL, 'credential update unavailable'); END;`); err != nil {
		t.Fatal(err)
	}
	request := f.request()
	if err := f.db.CompleteCognitoRefresh(ctx, request); err == nil {
		t.Fatal("credential update failure was hidden")
	}
	after, _ := readStoredCognitoRefresh(t, f)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed credential update changed protected credentials")
	}
	if _, err := f.db.db.ExecContext(ctx, "DROP TRIGGER reject_refresh_update"); err != nil {
		t.Fatal(err)
	}
	if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("failed credential update was retried: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("failed credential update restored session access: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
		t.Fatalf("failed credential update lost its durable reservation: count=%d err=%v", len(events), err)
	}
}

func TestCognitoRefreshCompletionRechecksQueuedAndCommittedExpiry(t *testing.T) {
	for _, tt := range []struct {
		name       string
		stage      int
		generation int64
		outcome    AuditOutcome
	}{
		{"queued", 0, 1, AuditFailed},
		{"before commit", 2, 1, AuditFailed},
		{"after commit", 3, 2, AuditSucceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
			request := f.request()
			ctx := context.Background()
			var err error
			if tt.stage == 0 {
				err = runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(f.details.ValidUntil.UnixNano())
				}, func() error {
					return f.db.CompleteCognitoRefresh(ctx, request)
				})
			} else {
				calls := 0
				f.db.now = func() time.Time {
					calls++
					if calls >= tt.stage {
						f.current.Store(f.details.ValidUntil.UnixNano())
					}
					return f.clock()
				}
				err = f.db.CompleteCognitoRefresh(ctx, request)
			}
			if !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("expired completion was accepted: %v", err)
			}
			stored, exists := readStoredCognitoRefresh(t, f)
			if !exists || stored.generation != tt.generation {
				t.Fatalf("unexpected generation after expiration: %d", stored.generation)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != tt.outcome {
				t.Fatalf("expiration outcome was not durable: count=%d err=%v", len(events), err)
			}
			if _, err := f.manager.Resolve(ctx, f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("expired completion granted session access: %v", err)
			}
			if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("expired completion proof was reused: %v", err)
			}
		})
	}
}

func TestCognitoRefreshCompletionCancellationConsumesProof(t *testing.T) {
	f := newCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := f.request()
	if err := f.db.CompleteCognitoRefresh(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled completion did not report cancellation: %v", err)
	}
	if err := f.db.CompleteCognitoRefresh(context.Background(), request); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("canceled completion proof was retried: %v", err)
	}
	if _, err := f.manager.Resolve(context.Background(), f.credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("canceled completion restored session access: %v", err)
	}
}
