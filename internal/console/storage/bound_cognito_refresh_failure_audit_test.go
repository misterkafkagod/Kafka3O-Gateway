package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func boundCognitoRefreshFailure(request cognito.BoundRefreshDispatchRequest, status cognito.RefreshFailureStatus, now time.Time) cognito.RefreshFailure {
	return cognito.RefreshFailure{
		OperationID: request.Reservation.OperationID, SessionID: request.Reservation.SessionID,
		Generation: request.Reservation.Generation, OccurredAt: now, Status: status,
	}
}

func TestBoundCognitoRefreshFailureIsDurableAndRedacted(t *testing.T) {
	for _, tt := range []struct {
		status  cognito.RefreshFailureStatus
		outcome AuditOutcome
	}{
		{cognito.RefreshFailedBeforeSend, AuditFailedBeforeSend},
		{cognito.RefreshOutcomeUnknown, AuditUnknown},
	} {
		t.Run(string(tt.status), func(t *testing.T) {
			f, request := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			failure := boundCognitoRefreshFailure(request, tt.status, f.clock())
			wrong := sha256.Sum256([]byte("other browser marker"))
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, [32]byte{}); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("bound failure accepted a missing marker: %v", err)
			}
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, wrong); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("bound failure accepted the wrong marker: %v", err)
			}
			if err := f.db.RecordCognitoRefreshFailure(ctx, failure); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("unbound failure consumed a bound reservation: %v", err)
			}
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding); err != nil {
				t.Fatalf("bound refresh failure was not recorded: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != tt.outcome ||
				events[1].OperationID != failure.OperationID || events[1].ActorID != request.Reservation.IdentityID ||
				events[1].ResourceID != failure.SessionID {
				t.Fatalf("bound refresh failure audit was incorrect: count=%d err=%v", len(events), err)
			}
			serialized, err := json.Marshal(failure)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
				fixtureRefresh, "opaque|synthetic-subject", f.provider.Issuer, "unmapped-group"} {
				if strings.Contains(string(serialized), secret) || strings.Contains(fmt.Sprint(events), secret) {
					t.Fatal("bound refresh failure exposed sensitive material")
				}
			}
			f.reopen(t)
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding); !errors.Is(err, cognito.ErrRefreshAudit) {
				t.Fatalf("bound refresh failure outcome was replayed after restart: %v", err)
			}
			state, err := f.db.InspectBoundCognitoRefresh(ctx, request.TokenHash, f.binding, f.clock())
			if err != nil || state != session.CognitoRefreshBlocked || f.refreshCalls.Load() != 0 {
				t.Fatalf("audited failure revived a reserved session or called the provider: state=%d err=%v", state, err)
			}
		})
	}
}

func TestBoundCognitoRefreshFailureRecordsInactiveOriginal(t *testing.T) {
	for _, mode := range []string{"removed", "replaced", "revoked", "disabled", "expired", "missing-browser-table"} {
		t.Run(mode, func(t *testing.T) {
			f, request := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			var replacement session.Record
			switch mode {
			case "removed":
				if _, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", f.binding[:]); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", request.Reservation.IdentityID); err != nil {
					t.Fatal(err)
				}
				replacement, _ = browserSessionRecord("failure-replacement", "browser-local-b", f.binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", request.Reservation.IdentityID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := f.db.RevokeBoundSession(ctx, request.TokenHash, f.binding, "revoke-before-bound-failure", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", request.Reservation.IdentityID); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
			case "missing-browser-table":
				if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
					t.Fatal(err)
				}
			}
			failure := boundCognitoRefreshFailure(request, cognito.RefreshOutcomeUnknown, f.clock())
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding); err != nil {
				t.Fatalf("inactive original reservation could not be audited: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != AuditUnknown ||
				events[1].ResourceID != request.Reservation.SessionID || f.refreshCalls.Load() != 0 {
				t.Fatalf("inactive original failure audit was incorrect: count=%d err=%v", len(events), err)
			}
			if mode == "replaced" {
				principal, err := f.db.UseBoundSession(ctx, replacement.TokenHash, f.binding, f.clock, session.DefaultIdleLifetime, nil)
				if err != nil || principal.SessionID != replacement.SessionID {
					t.Fatalf("recording the original failure affected its replacement: %v", err)
				}
			}
		})
	}
}

func TestBoundCognitoRefreshFailureRejectsChangedReservation(t *testing.T) {
	f, request := newBoundCognitoRefreshDispatchFixture(t)
	ctx := context.Background()
	failure := boundCognitoRefreshFailure(request, cognito.RefreshFailedBeforeSend, f.clock())
	for _, tt := range []struct {
		name   string
		change func(*cognito.RefreshFailure)
	}{
		{"operation", func(v *cognito.RefreshFailure) {
			v.OperationID = base64.RawURLEncoding.EncodeToString(make([]byte, 16))
		}},
		{"session", func(v *cognito.RefreshFailure) { v.SessionID = base64.RawURLEncoding.EncodeToString(make([]byte, 16)) }},
		{"generation", func(v *cognito.RefreshFailure) { v.Generation++ }},
		{"status", func(v *cognito.RefreshFailure) { v.Status = "unexpected" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			changed := failure
			tt.change(&changed)
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, changed, f.binding); err == nil {
				t.Fatal("changed bound failure was accepted")
			}
		})
	}
	if _, err := f.db.db.Exec("UPDATE audit_events SET decision = 'denied' WHERE event_id = ?",
		cognitoRefreshEventID(failure.SessionID, failure.Generation)); err != nil {
		t.Fatal(err)
	}
	if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("bound failure accepted a non-allowed attempt: %v", err)
	}
	if _, err := f.db.db.Exec("UPDATE audit_events SET decision = 'allowed' WHERE event_id = ?",
		cognitoRefreshEventID(failure.SessionID, failure.Generation)); err != nil {
		t.Fatal(err)
	}
	other := sha256.Sum256([]byte("changed session marker"))
	if _, err := f.db.db.Exec("UPDATE console_sessions SET browser_binding_hash = ? WHERE id = ?", other[:], failure.SessionID); err != nil {
		t.Fatal(err)
	}
	if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
		t.Fatalf("failure audit accepted a changed historical marker: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
		t.Fatal("invalid bound failure changed the reservation")
	}
}

func TestBoundCognitoRefreshFailureIsRecordedAtMostOnce(t *testing.T) {
	f, request := newBoundCognitoRefreshDispatchFixture(t)
	failure := boundCognitoRefreshFailure(request, cognito.RefreshOutcomeUnknown, f.clock())
	const callers = 8
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- f.db.RecordBoundCognitoRefreshFailure(context.Background(), failure, f.binding)
		}()
	}
	wait.Wait()
	close(results)
	success, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, cognito.ErrRefreshAudit):
			rejected++
		default:
			t.Fatalf("unexpected bound failure result: %v", err)
		}
	}
	if success != 1 || rejected != callers-1 || f.refreshCalls.Load() != 0 {
		t.Fatalf("bound failure was not recorded exactly once: success=%d rejected=%d", success, rejected)
	}
}

func TestBoundCognitoRefreshFailurePreservesUnboundAudit(t *testing.T) {
	for _, deleteSession := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted-session=%t", deleteSession), func(t *testing.T) {
			f, _ := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			record, token, protector := browserCognitoRecord(t, f.cognitoCodeFixture, "unbound-failure", [32]byte{})
			record.Session.SessionID = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{83}, 16))
			principal, err := f.db.CreateCognitoSession(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			request := cognitoRefreshRequest(t, session.Credentials{Principal: principal, Token: token}, protector, f.clock())
			reservation, err := f.db.ReserveCognitoRefresh(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			failure := cognito.RefreshFailure{
				OperationID: reservation.OperationID, SessionID: reservation.SessionID, Generation: reservation.Generation,
				OccurredAt: f.clock(), Status: cognito.RefreshOutcomeUnknown,
			}
			if err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding); !errors.Is(err, cognito.ErrRefreshUnavailable) {
				t.Fatalf("bound failure consumed an unbound reservation: %v", err)
			}
			if deleteSession {
				if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", reservation.SessionID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.db.Exec("DELETE FROM console_sessions WHERE id = ?", reservation.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
				t.Fatal(err)
			}
			if err := f.db.RecordCognitoRefreshFailure(ctx, failure); err != nil {
				t.Fatalf("unbound failure depended on browser storage or a deleted session: %v", err)
			}
		})
	}
}
