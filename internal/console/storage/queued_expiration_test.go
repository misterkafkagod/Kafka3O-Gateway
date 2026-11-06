package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func runQueuedDatabaseOperation(t *testing.T, db *Database, advance func(), operation func() error) error {
	t.Helper()
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	waitCount := db.db.Stats().WaitCount
	done := make(chan error, 1)
	go func() { done <- operation() }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for db.db.Stats().WaitCount == waitCount {
		select {
		case err := <-done:
			t.Fatalf("operation did not wait for the connection: %v", err)
		case <-timer.C:
			t.Fatal("operation did not reach the connection queue")
		case <-ticker.C:
		}
	}
	advance()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		return err
	case <-timer.C:
		t.Fatal("queued operation did not finish")
		return nil
	}
}

func TestQueuedSessionResolutionHonorsExpiry(t *testing.T) {
	for _, tt := range []struct {
		name           string
		idle, absolute time.Duration
		elapsed        time.Duration
	}{
		{"idle", time.Minute, time.Hour, time.Minute},
		{"absolute", 2 * time.Minute, 2 * time.Minute, 2 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			start := time.Now().UTC().Add(time.Second)
			var current atomic.Int64
			current.Store(start.UnixNano())
			clock := func() time.Time { return time.Unix(0, current.Load()).UTC() }
			manager, err := session.NewManager(f.db, rand.Reader, clock, tt.idle, tt.absolute)
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := manager.IssueLocal(ctx, f.admin)
			if err != nil {
				t.Fatal(err)
			}
			err = runQueuedDatabaseOperation(t, f.db, func() {
				current.Store(start.Add(tt.elapsed).UnixNano())
			}, func() error {
				_, err := manager.Resolve(ctx, credentials.Token)
				return err
			})
			if !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("queued expired session was accepted: %v", err)
			}
			var idleExpiry int64
			if err := f.db.db.QueryRowContext(ctx, "SELECT idle_expires_at_ns FROM console_sessions WHERE id = ?", credentials.Principal.SessionID).Scan(&idleExpiry); err != nil {
				t.Fatal(err)
			}
			if idleExpiry != credentials.Principal.IdleExpiresAt.UnixNano() {
				t.Fatal("expired session was extended")
			}
		})
	}
}

func TestQueuedSessionResolutionExtendsIdleFromDecisionTime(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	start := time.Now().UTC().Add(time.Second)
	var current atomic.Int64
	current.Store(start.UnixNano())
	clock := func() time.Time { return time.Unix(0, current.Load()).UTC() }
	manager, err := session.NewManager(f.db, rand.Reader, clock, 10*time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := manager.IssueLocal(ctx, f.admin)
	if err != nil {
		t.Fatal(err)
	}
	var principal session.Principal
	err = runQueuedDatabaseOperation(t, f.db, func() {
		current.Store(start.Add(2 * time.Minute).UnixNano())
	}, func() error {
		var err error
		principal, err = manager.Resolve(ctx, credentials.Token)
		return err
	})
	if err != nil || !principal.IdleExpiresAt.Equal(start.Add(12*time.Minute)) {
		t.Fatalf("queued valid session did not extend from decision time: idle=%v err=%v", principal.IdleExpiresAt, err)
	}
}

func TestQueuedAdministratorReservationUsesDecisionTime(t *testing.T) {
	for _, action := range []string{"create", "reset"} {
		t.Run(action, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			start := time.Now().UTC().Add(time.Second)
			var current atomic.Int64
			current.Store(start.UnixNano())
			f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
			var operation func() error
			table, auditAction := "local_account_creation_attempts", "console.local_account.create"
			if action == "create" {
				operation = func() error {
					return f.db.ReserveLocalAccountCreation(ctx, identity.LocalAccountCreationAttempt{
						Actor: actor, AccountID: "new-account", OperationID: "operation", OccurredAt: start,
					})
				}
			} else {
				table, auditAction = "password_reset_attempts", "console.local_password_reset"
				operation = func() error {
					return f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
						Actor: actor, TargetID: target.ID, OperationID: "operation", OccurredAt: start,
					})
				}
			}
			decisionAt := start.Add(30 * time.Second)
			err := runQueuedDatabaseOperation(t, f.db, func() {
				current.Store(decisionAt.UnixNano())
			}, operation)
			if err != nil {
				t.Fatalf("queued valid reservation was rejected: %v", err)
			}
			var occurredAt int64
			query := fmt.Sprintf("SELECT occurred_at_ns FROM %s WHERE operation_id = ?", table)
			if err := f.db.db.QueryRowContext(ctx, query, "operation").Scan(&occurredAt); err != nil || occurredAt != decisionAt.UnixNano() {
				t.Fatalf("reservation time=%d, expected=%d: %v", occurredAt, decisionAt.UnixNano(), err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: auditAction, Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || !events[0].OccurredAt.Equal(decisionAt) {
				t.Fatalf("reservation audit did not use decision time: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestQueuedAdministratorReservationsRecheckSessionExpiry(t *testing.T) {
	for _, action := range []string{"create", "reset", "disable"} {
		t.Run(action, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			start := time.Now().UTC().Add(time.Second)
			var current atomic.Int64
			current.Store(start.UnixNano())
			f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
			if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?",
				start.Add(time.Minute).UnixNano(), actor.SessionID); err != nil {
				t.Fatal(err)
			}
			var auditAction string
			operation := func() error {
				switch action {
				case "create":
					return f.db.ReserveLocalAccountCreation(ctx, identity.LocalAccountCreationAttempt{
						Actor: actor, AccountID: "new-account", OperationID: "operation", OccurredAt: start,
					})
				case "reset":
					return f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
						Actor: actor, TargetID: target.ID, OperationID: "operation", OccurredAt: start,
					})
				default:
					return f.db.SetLocalAccountEnabled(ctx, identity.LocalAccountStateChange{
						Actor: actor, TargetID: target.ID, Enabled: false, OperationID: "operation",
						EventID: "outcome", RevocationEventID: "revocation", OccurredAt: start,
					})
				}
			}
			switch action {
			case "create":
				auditAction = "console.local_account.create"
			case "reset":
				auditAction = "console.local_password_reset"
			default:
				auditAction = "console.local_account.disable"
			}
			err := runQueuedDatabaseOperation(t, f.db, func() {
				current.Store(start.Add(time.Minute).UnixNano())
			}, operation)
			if !errors.Is(err, identity.ErrInvalidAdminSession) {
				t.Fatalf("expired administrator was accepted: %v", err)
			}
			stored, err := f.db.FindLocalAccount(ctx, target.Username)
			if err != nil || !stored.Enabled || stored.CredentialVersion != target.CredentialVersion {
				t.Fatalf("denied operation changed the target: %v", err)
			}
			var reservations int
			if err := f.db.db.QueryRowContext(ctx, `SELECT
(SELECT COUNT(*) FROM local_account_creation_attempts) + (SELECT COUNT(*) FROM password_reset_attempts)`).Scan(&reservations); err != nil || reservations != 0 {
				t.Fatalf("expired administrator reserved an operation: count=%d err=%v", reservations, err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: auditAction, Limit: 10})
			if err != nil || len(events) == 0 || events[len(events)-1].Outcome != AuditDenied ||
				!events[len(events)-1].OccurredAt.Equal(start.Add(time.Minute)) {
				t.Fatalf("expired administrator denial was not audited at decision time: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestQueuedAdministratorCompletionsRecheckExpiry(t *testing.T) {
	for _, action := range []string{"create", "reset"} {
		for _, expiry := range []string{"session", "reservation"} {
			t.Run(action+"/"+expiry, func(t *testing.T) {
				f := newAccountAdministrationFixture(t)
				ctx := context.Background()
				target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
				actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
				start := time.Now().UTC().Add(time.Second)
				var current atomic.Int64
				current.Store(start.UnixNano())
				f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
				if expiry == "session" {
					if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?",
						start.Add(time.Minute).UnixNano(), actor.SessionID); err != nil {
						t.Fatal(err)
					}
				}
				hash, err := identity.HashLocalPassword(rand.Reader, "temporary passphrase")
				if err != nil {
					t.Fatal(err)
				}
				var operation func() error
				var auditAction string
				if action == "create" {
					if err := f.db.ReserveLocalAccountCreation(ctx, identity.LocalAccountCreationAttempt{
						Actor: actor, AccountID: "new-account", OperationID: "operation", OccurredAt: start,
					}); err != nil {
						t.Fatal(err)
					}
					operation = func() error {
						return f.db.CompleteLocalAccountCreation(ctx, identity.LocalAccountCreationCompletion{
							Actor: actor, Account: identity.LocalAccount{
								ID: "new-account", Username: "new-user", PasswordHash: hash,
								Enabled: true, MustChangePassword: true, CredentialVersion: 1,
							},
							OperationID: "operation", EventID: "outcome", OccurredAt: start,
						})
					}
					auditAction = "console.local_account.create"
				} else {
					if err := f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
						Actor: actor, TargetID: target.ID, OperationID: "operation", OccurredAt: start,
					}); err != nil {
						t.Fatal(err)
					}
					operation = func() error {
						return f.db.CompleteLocalPasswordReset(ctx, identity.LocalPasswordResetCompletion{
							Actor: actor, TargetID: target.ID, OperationID: "operation", EventID: "outcome",
							RevocationEventID: "revocation", NewHash: hash, OccurredAt: start,
						})
					}
					auditAction = "console.local_password_reset"
				}
				elapsed, want := time.Minute, identity.ErrInvalidAdminSession
				if expiry == "reservation" {
					elapsed = 15 * time.Minute
					if action == "create" {
						want = identity.ErrAccountCreationExpired
					} else {
						want = identity.ErrPasswordResetExpired
					}
				}
				err = runQueuedDatabaseOperation(t, f.db, func() {
					current.Store(start.Add(elapsed).UnixNano())
				}, operation)
				if !errors.Is(err, want) {
					t.Fatalf("queued %s accepted expired %s: %v", action, expiry, err)
				}
				if _, err := f.db.FindLocalAccount(ctx, "new-user"); !errors.Is(err, identity.ErrAccountNotFound) {
					t.Fatalf("denied creation persisted an account: %v", err)
				}
				stored, err := f.db.FindLocalAccount(ctx, target.Username)
				if err != nil || stored.CredentialVersion != target.CredentialVersion {
					t.Fatalf("denied completion changed the target: %v", err)
				}
				events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: auditAction, Limit: 10})
				if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied ||
					!events[1].OccurredAt.Equal(start.Add(elapsed)) {
					t.Fatalf("expired completion was not audited at decision time: count=%d err=%v", len(events), err)
				}
				if err := operation(); !errors.Is(err, identity.ErrInvalidAccountCreation) && !errors.Is(err, identity.ErrInvalidPasswordReset) {
					t.Fatalf("denied completion was replayed: %v", err)
				}
			})
		}
	}
}

func TestQueuedRejectedOperationsHonorReservationExpiry(t *testing.T) {
	for _, action := range []string{"create", "reset"} {
		t.Run(action, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			start := time.Now().UTC().Add(time.Second)
			var current atomic.Int64
			current.Store(start.UnixNano())
			f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
			var operation func() error
			var want error
			auditAction := "console.local_account.create"
			if action == "create" {
				if err := f.db.ReserveLocalAccountCreation(ctx, identity.LocalAccountCreationAttempt{
					Actor: actor, AccountID: "new-account", OperationID: "operation", OccurredAt: start,
				}); err != nil {
					t.Fatal(err)
				}
				operation = func() error {
					return f.db.FailLocalAccountCreation(ctx, identity.LocalAccountCreationFailure{
						Actor: actor, AccountID: "new-account", OperationID: "operation",
						EventID: "outcome", OccurredAt: start, Rejected: true,
					})
				}
				want = identity.ErrAccountCreationExpired
			} else {
				if err := f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
					Actor: actor, TargetID: target.ID, OperationID: "operation", OccurredAt: start,
				}); err != nil {
					t.Fatal(err)
				}
				operation = func() error {
					return f.db.FailLocalPasswordReset(ctx, identity.LocalPasswordResetFailure{
						Actor: actor, TargetID: target.ID, OperationID: "operation",
						EventID: "outcome", OccurredAt: start, Rejected: true,
					})
				}
				want, auditAction = identity.ErrPasswordResetExpired, "console.local_password_reset"
			}
			err := runQueuedDatabaseOperation(t, f.db, func() {
				current.Store(start.Add(15 * time.Minute).UnixNano())
			}, operation)
			if !errors.Is(err, want) {
				t.Fatalf("queued failure ignored reservation expiry: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: auditAction, Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied {
				t.Fatalf("expired rejection was not audited as denied: count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestTransactionTimeRejectsInvalidClockAndDoesNotMoveBackward(t *testing.T) {
	requested := time.Now().UTC()
	db := &Database{now: func() time.Time { return requested.Add(-time.Minute) }}
	if got, err := db.transactionTime(requested); err != nil || !got.Equal(requested) {
		t.Fatalf("transaction time moved backward: %v %v", got, err)
	}
	db.now = func() time.Time { return requested.Add(time.Minute) }
	if got, err := db.transactionTime(requested); err != nil || !got.Equal(requested.Add(time.Minute)) {
		t.Fatalf("transaction time ignored the current clock: %v %v", got, err)
	}
	for _, invalid := range []time.Time{{}, time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC)} {
		db.now = func() time.Time { return invalid }
		if _, err := db.transactionTime(requested); !errors.Is(err, ErrInvalidAuditEvent) {
			t.Fatalf("invalid transaction clock %s: %v", fmt.Sprint(invalid), err)
		}
	}
}
