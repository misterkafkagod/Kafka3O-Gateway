package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func TestBoundCognitoRefreshExchangeNeverRetriesProviderFailure(t *testing.T) {
	for _, mode := range []string{"refresh outage", "refresh disconnect", "refresh redirect", "refresh forged", "refresh wrong nonce", "refresh missing ID", "refresh invalid token"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			f.mu.Lock()
			f.mode = mode
			f.mu.Unlock()
			result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector)
			if err == nil || result.OperationID == "" || f.refreshCalls.Load() != 1 || f.redirectCalls.Load() != 0 {
				t.Fatalf("failed provider refresh returned success or retried: %v", err)
			}
			if _, err := result.ForCompletion(f.protector, f.clock()); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("failed provider refresh returned a proof: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditUnknown ||
				events[1].OperationID != result.OperationID {
				t.Fatalf("provider failure was not durably marked unknown: count=%d err=%v", len(events), err)
			}
			if _, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
				f.refreshCalls.Load() != 1 {
				t.Fatalf("uncertain bound provider refresh was replayed: %v", err)
			}
		})
	}
}

func TestBoundCognitoRefreshExchangeFailureRemainsBlockedAfterRestart(t *testing.T) {
	f := newBoundCognitoRefreshFixture(t)
	ctx := context.Background()
	f.mu.Lock()
	f.mode = "refresh outage"
	f.mu.Unlock()
	if _, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector); !errors.Is(err, cognito.ErrRefreshUnknown) {
		t.Fatalf("provider outage was not uncertain: %v", err)
	}
	f.reopen(t)
	exchanger, err := cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector); !errors.Is(err, cognito.ErrRefreshUnavailable) ||
		f.refreshCalls.Load() != 1 {
		t.Fatalf("uncertain bound refresh was replayed after restart: %v", err)
	}
}

func TestBoundCognitoRefreshExchangeRejectsChangedCompletionState(t *testing.T) {
	for _, mode := range []string{"mapping", "generation", "nonce", "ciphertext", "groups", "issuer", "outcome", "missing-reservation", "missing-browser-table"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			request := f.completionRequest()
			want := error(cognito.ErrRefreshState)
			switch mode {
			case "mapping":
				_, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", f.binding[:])
				if err != nil {
					t.Fatal(err)
				}
				want = cognito.ErrRefreshUnavailable
			case "generation":
				_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET refresh_generation = 2 WHERE session_id = ?", f.credentials.Principal.SessionID)
				if err != nil {
					t.Fatal(err)
				}
			case "nonce":
				value := sha256.Sum256([]byte("changed nonce"))
				_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET original_nonce_hash = ? WHERE session_id = ?", value[:], f.credentials.Principal.SessionID)
				if err != nil {
					t.Fatal(err)
				}
			case "ciphertext":
				_, err := f.db.db.Exec("UPDATE cognito_session_credentials SET encrypted_refresh_token = ? WHERE session_id = ?",
					bytes.Repeat([]byte{1}, 32), f.credentials.Principal.SessionID)
				if err != nil {
					t.Fatal(err)
				}
			case "groups":
				_, err := f.db.db.Exec(`UPDATE cognito_session_credentials SET groups_json = '["changed-group"]' WHERE session_id = ?`,
					f.credentials.Principal.SessionID)
				if err != nil {
					t.Fatal(err)
				}
			case "issuer":
				_, err := f.db.db.Exec("UPDATE cognito_identities SET issuer = 'https://other.invalid' WHERE identity_id = ?", f.credentials.Principal.AccountID)
				if err != nil {
					t.Fatal(err)
				}
			case "outcome":
				if err := f.db.RecordBoundCognitoRefreshFailure(ctx, cognito.RefreshFailure{
					OperationID: f.result.OperationID, SessionID: f.credentials.Principal.SessionID,
					Generation: 1, OccurredAt: f.clock(), Status: cognito.RefreshOutcomeUnknown,
				}, f.binding); err != nil {
					t.Fatal(err)
				}
				want = cognito.ErrRefreshUnavailable
			case "missing-reservation":
				_, err := f.db.db.Exec("DELETE FROM audit_events WHERE event_id = ?",
					cognitoRefreshEventID(f.credentials.Principal.SessionID, 1))
				if err != nil {
					t.Fatal(err)
				}
				want = cognito.ErrRefreshUnavailable
			case "missing-browser-table":
				_, err := f.db.db.Exec("DROP TABLE console_browser_sessions")
				if err != nil {
					t.Fatal(err)
				}
				want = nil
			}
			var beforeGeneration int64
			var beforeEncrypted []byte
			if err := f.db.db.QueryRow("SELECT refresh_generation, encrypted_refresh_token FROM cognito_session_credentials WHERE session_id = ?",
				f.credentials.Principal.SessionID).Scan(&beforeGeneration, &beforeEncrypted); err != nil {
				t.Fatal(err)
			}
			err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding)
			if (want != nil && !errors.Is(err, want)) || (want == nil && (err == nil || errors.Is(err, cognito.ErrRefreshUnavailable))) {
				t.Fatalf("changed bound completion state was accepted or hidden: %v", err)
			}
			var generation int64
			var encrypted []byte
			err = f.db.db.QueryRow("SELECT refresh_generation, encrypted_refresh_token FROM cognito_session_credentials WHERE session_id = ?",
				f.credentials.Principal.SessionID).Scan(&generation, &encrypted)
			if err != nil {
				t.Fatal(err)
			}
			if generation != beforeGeneration || !bytes.Equal(encrypted, beforeEncrypted) || f.refreshCalls.Load() != 1 {
				t.Fatal("changed completion state persisted or retried a provider credential")
			}
			if err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("changed bound completion state allowed proof replay: %v", err)
			}
		})
	}
}

func TestBoundCognitoRefreshExchangeWithholdsCompletionOnAuditFailure(t *testing.T) {
	for _, tt := range []struct {
		outcome string
		commit  bool
	}{
		{"succeeded", false},
		{"succeeded", true},
		{"failed", false},
		{"failed", true},
	} {
		t.Run(fmt.Sprintf("%s-commit=%t", tt.outcome, tt.commit), func(t *testing.T) {
			f := newBoundCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			if tt.outcome == "failed" {
				if _, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", f.binding[:]); err != nil {
					t.Fatal(err)
				}
			}
			trigger := `CREATE TRIGGER reject_bound_refresh_completion BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = '` + tt.outcome + `'
BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`
			want := cognito.ErrRefreshAudit
			if tt.commit {
				trigger = `CREATE TABLE bound_refresh_completion_commit (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER reject_bound_refresh_completion AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = '` + tt.outcome + `'
BEGIN INSERT INTO bound_refresh_completion_commit (identity_id) VALUES ('missing-identity'); END;`
				want = cognito.ErrRefreshUnknown
			}
			if _, err := f.db.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			request := f.completionRequest()
			err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding)
			if !errors.Is(err, want) || errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
				t.Fatalf("bound completion hid an audit or commit failure: %v", err)
			}
			var generation int64
			if err := f.db.db.QueryRow("SELECT refresh_generation FROM cognito_session_credentials WHERE session_id = ?",
				f.credentials.Principal.SessionID).Scan(&generation); err != nil || generation != 1 {
				t.Fatal("failed completion changed stored credentials")
			}
			if err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
				t.Fatalf("failed completion reused a proof or provider request: %v", err)
			}
		})
	}
}

func TestBoundCognitoRefreshExchangeRejectsCompletionClockRegression(t *testing.T) {
	for _, mode := range []string{"after-request", "during", "after-commit", "expiry-after-commit"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoCompletionFixture(t, "refresh rotated", cognito.ClientAuthenticationPublic)
			ctx := context.Background()
			request := f.completionRequest()
			details, err := f.proof.Details(f.protector, f.clock())
			if err != nil {
				t.Fatal(err)
			}
			base := f.clock()
			calls := 0
			f.db.now = func() time.Time {
				calls++
				threshold := 1
				switch mode {
				case "during":
					threshold = 2
				case "after-commit", "expiry-after-commit":
					threshold = 3
				}
				if calls >= threshold {
					if mode == "expiry-after-commit" {
						return details.ValidUntil
					}
					return base.Add(-time.Nanosecond)
				}
				return base
			}
			err = f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding)
			if !errors.Is(err, cognito.ErrRefreshUnknown) || errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
				t.Fatalf("bound completion hid %s clock outcome: %v", mode, err)
			}
			var generation int64
			if err := f.db.db.QueryRow("SELECT refresh_generation FROM cognito_session_credentials WHERE session_id = ?",
				f.credentials.Principal.SessionID).Scan(&generation); err != nil {
				t.Fatal(err)
			}
			committed := mode == "after-commit" || mode == "expiry-after-commit"
			if (generation == 2) != committed {
				t.Fatal("clock outcome did not reflect the committed refresh state")
			}
			if err := f.db.CompleteBoundCognitoRefresh(ctx, request, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
				t.Fatalf("clock failure reused a completion proof: %v", err)
			}
		})
	}
}

func TestBoundCognitoRefreshExchangeRejectsFutureIssuedClaims(t *testing.T) {
	f := newBoundCognitoCompletionFixture(t, "refresh future issuance", cognito.ClientAuthenticationPublic)
	ctx := context.Background()
	err := f.db.CompleteBoundCognitoRefresh(ctx, f.completionRequest(), f.binding)
	if !errors.Is(err, cognito.ErrRefreshUnknown) || !errors.Is(err, ErrInvalidAuditEvent) || f.refreshCalls.Load() != 1 {
		t.Fatalf("bound completion accepted claims issued in the future: %v", err)
	}
	var generation int64
	if err := f.db.db.QueryRow("SELECT refresh_generation FROM cognito_session_credentials WHERE session_id = ?",
		f.credentials.Principal.SessionID).Scan(&generation); err != nil || generation != 1 {
		t.Fatal("future claims changed the stored generation")
	}
}

func TestBoundCognitoRefreshExchangeConsumesProofAfterWrongTime(t *testing.T) {
	f := newBoundCognitoCompletionFixture(t, "refresh no rotation", cognito.ClientAuthenticationPublic)
	request := f.completionRequest()
	request.OccurredAt = request.OccurredAt.Add(-time.Nanosecond)
	if err := f.db.CompleteBoundCognitoRefresh(context.Background(), request, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("bound completion accepted time before verification: %v", err)
	}
	if err := f.db.CompleteBoundCognitoRefresh(context.Background(), f.completionRequest(), f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 1 {
		t.Fatalf("invalid proof time allowed a replay: %v", err)
	}
}
