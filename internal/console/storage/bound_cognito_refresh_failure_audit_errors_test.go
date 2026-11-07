package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
)

func TestBoundCognitoRefreshFailureReportsAuditAndCommitFailures(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%t", commit), func(t *testing.T) {
			f, request := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			trigger := `CREATE TRIGGER reject_bound_refresh_failure BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'unknown'
BEGIN SELECT RAISE(FAIL, 'synthetic audit failure'); END;`
			want := cognito.ErrRefreshAudit
			if commit {
				trigger = `CREATE TABLE bound_refresh_failure_commit (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED);
CREATE TRIGGER reject_bound_refresh_failure AFTER INSERT ON audit_events
WHEN NEW.action = 'console.cognito.refresh' AND NEW.outcome = 'unknown'
BEGIN INSERT INTO bound_refresh_failure_commit (identity_id) VALUES ('missing-identity'); END;`
				want = cognito.ErrRefreshUnknown
			}
			if _, err := f.db.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			failure := boundCognitoRefreshFailure(request, cognito.RefreshOutcomeUnknown, f.clock())
			err := f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding)
			if !errors.Is(err, want) || errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 0 {
				t.Fatalf("bound failure hid an audit or commit error: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted {
				t.Fatal("failed audit or commit changed the durable reservation")
			}
		})
	}
}

func TestBoundCognitoRefreshFailureRejectsClockRegression(t *testing.T) {
	for _, mode := range []string{"before-session", "after-request", "during", "after-commit", "queued"} {
		t.Run(mode, func(t *testing.T) {
			f, request := newBoundCognitoRefreshDispatchFixture(t)
			ctx := context.Background()
			base := f.clock()
			failure := boundCognitoRefreshFailure(request, cognito.RefreshOutcomeUnknown, base)
			switch mode {
			case "before-session":
				f.current.Store(base.Add(-time.Nanosecond).UnixNano())
				failure.OccurredAt = f.clock()
			case "after-request":
				f.current.Store(base.Add(-time.Nanosecond).UnixNano())
			case "during", "after-commit":
				calls := 0
				f.db.now = func() time.Time {
					calls++
					threshold := 2
					if mode == "after-commit" {
						threshold = 3
					}
					if calls >= threshold {
						return base.Add(-time.Nanosecond)
					}
					return base
				}
			}
			run := func() error { return f.db.RecordBoundCognitoRefreshFailure(ctx, failure, f.binding) }
			var err error
			if mode == "queued" {
				err = runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(base.Add(-time.Nanosecond).UnixNano())
				}, run)
			} else {
				err = run()
			}
			want := error(ErrInvalidAuditEvent)
			if mode == "after-commit" {
				want = cognito.ErrRefreshUnknown
			}
			if !errors.Is(err, want) || errors.Is(err, cognito.ErrRefreshUnavailable) || f.refreshCalls.Load() != 0 {
				t.Fatalf("bound failure accepted %s clock: %v", mode, err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.cognito.refresh", Limit: 10})
			if err != nil || (len(events) == 2) != (mode == "after-commit") || events[0].Outcome != AuditAttempted {
				t.Fatal("bound failure clock had an unexpected durable outcome")
			}
		})
	}
}

func TestBoundCognitoRefreshFailureRejectsTimeBeforeReservation(t *testing.T) {
	for _, mode := range []string{"clock", "request"} {
		t.Run(mode, func(t *testing.T) {
			f := newBoundCognitoRefreshFixture(t)
			ctx := context.Background()
			base := f.clock()
			f.current.Store(base.Add(10 * time.Second).UnixNano())
			request := f.request(t)
			reservation, err := f.db.ReserveBoundCognitoRefresh(ctx, request, f.binding)
			if err != nil {
				t.Fatal(err)
			}
			earlier := base.Add(5 * time.Second)
			if mode == "clock" {
				f.current.Store(earlier.UnixNano())
			} else {
				f.current.Store(base.Add(12 * time.Second).UnixNano())
			}
			err = f.db.RecordBoundCognitoRefreshFailure(ctx, cognito.RefreshFailure{
				OperationID: reservation.OperationID, SessionID: reservation.SessionID, Generation: reservation.Generation,
				OccurredAt: earlier, Status: cognito.RefreshOutcomeUnknown,
			}, f.binding)
			if !errors.Is(err, ErrInvalidAuditEvent) || f.refreshCalls.Load() != 0 {
				t.Fatalf("bound failure accepted %s time before its reservation: %v", mode, err)
			}
		})
	}
}
