package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundRefreshReservationHookStore struct {
	*Database
	afterReservation func()
}

func (s *boundRefreshReservationHookStore) ReserveBoundCognitoRefresh(ctx context.Context, request cognito.RefreshReservationRequest, browserHash [32]byte) (cognito.RefreshReservation, error) {
	reservation, err := s.Database.ReserveBoundCognitoRefresh(ctx, request, browserHash)
	if err == nil {
		s.afterReservation()
	}
	return reservation, err
}

func TestBoundCognitoRefreshExchangeRechecksBeforeDispatch(t *testing.T) {
	for _, mode := range []string{"replaced", "revoked", "snapshot", "missing-browser-table", "clock"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			base := f.clock()
			var replacement session.Record
			store := &boundRefreshReservationHookStore{Database: f.db}
			store.afterReservation = func() {
				switch mode {
				case "replaced":
					if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
						t.Fatal(err)
					}
					replacement, _ = browserSessionRecord("before-dispatch-replacement", "browser-local-b", f.binding, f.clock())
					if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
						t.Fatal(err)
					}
				case "revoked":
					if err := f.db.RevokeBoundSession(ctx, f.record.Session.TokenHash, f.binding, "revoke-before-provider-send", f.clock()); err != nil {
						t.Fatal(err)
					}
				case "snapshot":
					if _, err := f.db.db.Exec(`UPDATE cognito_session_credentials SET groups_json = '["changed-group"]' WHERE session_id = ?`,
						f.credentials.Principal.SessionID); err != nil {
						t.Fatal(err)
					}
				case "missing-browser-table":
					if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
						t.Fatal(err)
					}
				case "clock":
					f.current.Store(base.Add(-time.Nanosecond).UnixNano())
				}
			}
			attempts, err := cognito.NewAttemptManager(store, rand.Reader, f.clock, f.key, f.provider, f.callback)
			if err != nil {
				t.Fatal(err)
			}
			exchanger, err := cognito.NewCodeExchanger(attempts, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
			if err != nil {
				t.Fatal(err)
			}
			result, exchangeErr := exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector)
			if exchangeErr == nil || result.OperationID == "" || f.refreshCalls.Load() != 0 {
				t.Fatalf("changed bound session reached the provider: %v", exchangeErr)
			}
			if _, proofErr := result.ForCompletion(f.protector, f.clock()); !errors.Is(proofErr, cognito.ErrRefreshUnavailable) {
				t.Fatalf("failed dispatch returned a completion proof: %v", proofErr)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) == 0 || events[0].Outcome != AuditAttempted {
				t.Fatal("bound provider reservation was not durable")
			}
			if mode == "clock" {
				if len(events) != 1 || !errors.Is(exchangeErr, cognito.ErrRefreshAudit) {
					t.Fatal("clock regression recorded a misleading outcome")
				}
			} else if len(events) != 2 || events[1].Outcome != AuditFailedBeforeSend {
				t.Fatal("failed bound dispatch was not audited before send")
			}
			if mode == "replaced" {
				principal, err := f.db.UseBoundSession(ctx, replacement.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, nil)
				if err != nil || principal.SessionID != replacement.SessionID {
					t.Fatalf("pre-dispatch denial affected the replacement: %v", err)
				}
			}
		})
	}
}

func TestBoundCognitoRefreshExchangeDeniesReplacementDuringProviderCall(t *testing.T) {
	for _, mode := range []string{"replaced", "replaced-outage", "revoked", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			f.mu.Lock()
			f.mode = "refresh rotated"
			if mode == "replaced-outage" {
				f.mode = "refresh outage"
			}
			entered, release := make(chan struct{}, 1), make(chan struct{})
			f.refreshEntered, f.refreshRelease = entered, release
			f.mu.Unlock()
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			ctx := context.Background()
			type exchangeResult struct {
				result cognito.RefreshExchangeResult
				err    error
			}
			done := make(chan exchangeResult, 1)
			go func() {
				result, err := f.exchanger.RefreshBoundCognitoSession(ctx, f.record.Session.TokenHash, f.binding, f.protector)
				done <- exchangeResult{result: result, err: err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("bound provider refresh did not start")
			}
			var replacement session.Record
			switch mode {
			case "replaced", "replaced-outage":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
				replacement, _ = browserSessionRecord("inflight-refresh-replacement", "browser-local-b", f.binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := f.db.RevokeBoundSession(ctx, f.record.Session.TokenHash, f.binding, "revoke-during-provider-refresh", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			var exchange exchangeResult
			select {
			case exchange = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("bound provider refresh did not finish")
			}
			if mode == "replaced-outage" {
				if !errors.Is(exchange.err, cognito.ErrRefreshUnknown) || f.refreshCalls.Load() != 1 {
					t.Fatalf("in-flight provider failure was not reported as unknown: %v", exchange.err)
				}
				if _, err := exchange.result.ForCompletion(f.protector, f.clock()); !errors.Is(err, cognito.ErrRefreshUnavailable) {
					t.Fatalf("failed provider request returned a proof: %v", err)
				}
				events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
				if err != nil || len(events) != 2 || events[1].Outcome != AuditUnknown {
					t.Fatal("provider failure after replacement was not audited")
				}
				var retained int
				if err := f.db.db.QueryRow("SELECT COUNT(*) FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID).Scan(&retained); err != nil || retained != 0 {
					t.Fatal("replacement retained the old provider credentials")
				}
				principal, err := f.db.UseBoundSession(ctx, replacement.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, nil)
				if err != nil || principal.SessionID != replacement.SessionID {
					t.Fatalf("provider failure affected the replacement: %v", err)
				}
				return
			}
			if exchange.err != nil || f.refreshCalls.Load() != 1 {
				t.Fatalf("fixture provider exchange failed unexpectedly: %v", exchange.err)
			}
			proof, err := exchange.result.ForCompletion(f.protector, f.clock())
			if err != nil {
				t.Fatal(err)
			}
			completion := cognito.RefreshCompletionRequest{Refresh: proof, Protector: f.protector, OccurredAt: f.clock()}
			if err := f.db.CompleteBoundCognitoRefresh(ctx, completion, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("bound completion restored an unavailable session: %v", err)
			}
			var generation int64
			err = f.db.db.QueryRow("SELECT refresh_generation FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID).Scan(&generation)
			if mode == "revoked" || mode == "replaced" {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatal("revocation or replacement retained provider credentials")
				}
			} else if err != nil || generation != 1 {
				t.Fatal("denied completion stored a rotated provider credential")
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditFailed {
				t.Fatal("denied in-flight completion was not audited")
			}
			if mode == "replaced" {
				principal, err := f.db.UseBoundSession(ctx, replacement.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, nil)
				if err != nil || principal.SessionID != replacement.SessionID {
					t.Fatalf("denied in-flight refresh affected the replacement: %v", err)
				}
				if _, err := f.db.UseBoundSession(ctx, f.record.Session.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, f.protector); err != session.ErrInvalidSession {
					t.Fatalf("replaced Cognito session revived: %v", err)
				}
			}
		})
	}
}
