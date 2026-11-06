package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestAdministratorPasswordResetRevokesSessionsAndRequiresChange(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, oldSession := f.addAccount(t, "subject-1", "other-user", "", "")
	secondSession, err := f.manager.IssueLocal(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	const temporaryPassword = "temporary reset passphrase"
	result, err := reset.Execute(ctx, actor, target.ID, temporaryPassword)
	if err != nil || result.OperationID == "" {
		t.Fatalf("password reset: operation=%q err=%v", result.OperationID, err)
	}
	if _, err := f.manager.Resolve(ctx, oldSession.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("old session survived password reset: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, secondSession.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("second old session survived password reset: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, target.Username)
	if err != nil || !stored.MustChangePassword || stored.CredentialVersion != 2 {
		t.Fatalf("reset account: mustChange=%t version=%d err=%v", stored.MustChangePassword, stored.CredentialVersion, err)
	}
	matches, err := identity.VerifyLocalPassword(temporaryPassword, stored.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("temporary password hash did not verify: %v", err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	client := netip.MustParseAddr("192.0.2.2")
	if _, err := authenticator.Execute(ctx, target.Username, "other strong passphrase", client); !errors.Is(err, identity.ErrAuthenticationFailed) {
		t.Fatalf("old password still worked: %v", err)
	}
	account, err := authenticator.Execute(ctx, target.Username, temporaryPassword, client)
	if err != nil || !account.MustChangePassword {
		t.Fatalf("temporary password login: %v", err)
	}
	newSession, err := f.manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.CheckSessionAccess(newSession.Principal, session.AccessNormal); !errors.Is(err, session.ErrPasswordChangeRequired) {
		t.Fatalf("temporary credential reached normal actions: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded ||
		events[0].EventID != result.OperationID || events[1].OperationID != result.OperationID ||
		events[1].ActorID != f.admin.ID || events[1].ResourceID != target.ID {
		t.Fatalf("reset audit count=%d err=%v", len(events), err)
	}
	revocations, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.sessions.revoke_on_password_reset", Limit: 10})
	if err != nil || len(revocations) != 1 || revocations[0].OperationID != result.OperationID {
		t.Fatalf("reset revocation audit count=%d err=%v", len(revocations), err)
	}
	all, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	serialized := fmt.Sprintf("%+v", all)
	for _, value := range []string{temporaryPassword, "other strong passphrase", "other-user", oldSession.Token, oldSession.CSRFToken} {
		if strings.Contains(serialized, value) {
			t.Fatal("password reset audit contains credentials or a username")
		}
	}
}

func TestPasswordResetRequiresCurrentGlobalAdmin(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: clusterAdmin.ID, SessionID: clusterSession.Principal.SessionID}
	for i := 0; i < passwordResetTargetLimit+1; i++ {
		if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); !errors.Is(err, identity.ErrGlobalAdminRequired) {
			t.Fatalf("cluster administrator reset attempt %d: %v", i, err)
		}
	}
	var count int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM password_reset_attempts").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unauthorized attempts consumed reset quota: count=%d err=%v", count, err)
	}
	actor.SessionID = "invalid-session"
	if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("invalid administrator session: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("denied resets affected the target session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 100})
	if err != nil || len(events) != passwordResetTargetLimit+2 ||
		events[0].ActorID != clusterAdmin.ID || events[0].Outcome != AuditDenied ||
		events[len(events)-1].ActorID != "anonymous" || events[len(events)-1].Outcome != AuditDenied {
		t.Fatalf("denied reset audit count=%d err=%v", len(events), err)
	}
}

func TestPasswordResetRejectsUntrustedTargetsWithoutLoggingThem(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		target string
		want   error
	}{
		{"untrusted-target-that-does-not-exist", identity.ErrAccountNotFound},
		{" ", identity.ErrInvalidLocalAccount},
		{strings.Repeat("x", 129), identity.ErrInvalidLocalAccount},
	} {
		if _, err := reset.Execute(ctx, actor, tt.target, "temporary reset passphrase"); !errors.Is(err, tt.want) {
			t.Fatalf("unexpected invalid target result: %v", err)
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 3 {
		t.Fatalf("invalid target audit count=%d err=%v", len(events), err)
	}
	for _, event := range events {
		if event.Outcome != AuditValidationRejected || event.ResourceID != "" || event.ActorID != f.admin.ID {
			t.Fatal("invalid target was logged or its rejection was not attributed")
		}
	}
	var rejected int
	err = f.db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM password_reset_attempts
WHERE completed = 1 AND credential_version IS NULL`).Scan(&rejected)
	if err != nil || rejected != len(events) {
		t.Fatalf("invalid targets did not consume administrator/global quota: count=%d err=%v", rejected, err)
	}
}

func TestInvalidPasswordResetTargetsConsumeAdministratorQuota(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	var firstOperation string
	for i := 0; i < passwordResetActorLimit; i++ {
		result, err := reset.Execute(ctx, actor, fmt.Sprintf("unknown-%d", i), "temporary reset passphrase")
		if !errors.Is(err, identity.ErrAccountNotFound) {
			t.Fatalf("unknown reset target %d: %v", i, err)
		}
		if i == 0 {
			firstOperation = result.OperationID
		}
	}
	if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); !errors.Is(err, identity.ErrPasswordResetThrottled) {
		t.Fatalf("unknown targets did not consume administrator quota: %v", err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "temporary reset passphrase")
	if err != nil {
		t.Fatal(err)
	}
	err = f.db.CompleteLocalPasswordReset(ctx, identity.LocalPasswordResetCompletion{
		Actor: actor, TargetID: "unknown-0", OperationID: firstOperation, EventID: "outcome",
		RevocationEventID: "revocation", NewHash: hash, OccurredAt: time.Now(),
	})
	if !errors.Is(err, identity.ErrInvalidPasswordReset) {
		t.Fatalf("invalid target reservation could be completed: %v", err)
	}
}

func TestPasswordResetRejectsAdministratorWhoMustChangePassword(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); !errors.Is(err, identity.ErrAdminPasswordChange) {
		t.Fatalf("temporary administrator credential performed reset: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("denied reset changed target session: %v", err)
	}
}

func TestPasswordResetThrottlingSurvivesReopenAndExpires(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	now := time.Now().Add(time.Second)
	clock := func() time.Time { return now }
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, clock)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < passwordResetTargetLimit; i++ {
		if _, err := reset.Execute(ctx, actor, target.ID, "short"); !errors.Is(err, identity.ErrInvalidPassword) {
			t.Fatalf("rejected reset %d: %v", i, err)
		}
	}
	path := f.db.path
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reset, err = identity.NewResetLocalPassword(db, rand.Reader, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); !errors.Is(err, identity.ErrPasswordResetThrottled) {
		t.Fatalf("throttle after reopening database: %v", err)
	}
	now = now.Add(passwordResetWindow)
	if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); err != nil {
		t.Fatalf("reset after window expiry: %v", err)
	}
	events, err := db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 100})
	if err != nil || len(events) != 2*passwordResetTargetLimit+3 {
		t.Fatalf("rate-limited reset audit count=%d err=%v", len(events), err)
	}
	for i := 0; i < passwordResetTargetLimit*2; i += 2 {
		if events[i].Outcome != AuditAttempted || events[i+1].Outcome != AuditValidationRejected ||
			events[i].EventID != events[i+1].OperationID {
			t.Fatalf("invalid password attempt %d was not correlated", i/2)
		}
	}
	if events[len(events)-3].Outcome != AuditDenied || events[len(events)-1].Outcome != AuditSucceeded {
		t.Fatal("throttled or successful reset audit is incorrect")
	}
}

func TestPasswordResetRateLimits(t *testing.T) {
	for _, tt := range []struct {
		name  string
		limit int
		scope string
	}{
		{"target", passwordResetTargetLimit, "target"},
		{"actor", passwordResetActorLimit, "actor"},
		{"global", passwordResetGlobalLimit, "global"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			at := time.Now().Add(time.Second)
			tx, err := f.db.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < tt.limit-1; i++ {
				actorID, targetID := actor.AccountID, target.ID
				if tt.scope != "actor" {
					actorID = fmt.Sprintf("other-actor-%d", i)
				}
				if tt.scope != "target" {
					targetID = fmt.Sprintf("other-target-%d", i)
				}
				actorKey, targetKey := sha256.Sum256([]byte("actor:"+actorID)), sha256.Sum256([]byte("target:"+targetID))
				_, err := tx.ExecContext(ctx, `INSERT INTO password_reset_attempts
(operation_id, actor_key, target_key, session_id, credential_version, occurred_at_ns, completed)
VALUES (?, ?, ?, 'fixture', 1, ?, 1)`, fmt.Sprintf("existing-%d", i), actorKey[:], targetKey[:], at.UnixNano())
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			attempt := identity.LocalPasswordResetAttempt{Actor: actor, TargetID: target.ID, OperationID: "last-allowed", OccurredAt: at}
			if err := f.db.ReserveLocalPasswordReset(ctx, attempt); err != nil {
				t.Fatalf("last permitted reset: %v", err)
			}
			attempt.OperationID = "over-limit"
			if err := f.db.ReserveLocalPasswordReset(ctx, attempt); !errors.Is(err, identity.ErrPasswordResetThrottled) {
				t.Fatalf("reset over %s limit: %v", tt.scope, err)
			}
		})
	}
}

func TestPasswordResetRechecksSessionAndBindsReservation(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	at := time.Now().Add(time.Second)
	attempt := identity.LocalPasswordResetAttempt{Actor: actor, TargetID: target.ID, OperationID: "reset-operation", OccurredAt: at}
	if err := f.db.ReserveLocalPasswordReset(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "temporary reset passphrase")
	if err != nil {
		t.Fatal(err)
	}
	completion := identity.LocalPasswordResetCompletion{
		Actor: actor, TargetID: target.ID, OperationID: attempt.OperationID, EventID: "reset-outcome",
		RevocationEventID: "reset-revocation", NewHash: hash, OccurredAt: at.Add(time.Second),
	}
	completion.TargetID = "different-target"
	if err := f.db.CompleteLocalPasswordReset(ctx, completion); !errors.Is(err, identity.ErrInvalidPasswordReset) {
		t.Fatalf("reservation accepted a different target: %v", err)
	}
	completion.TargetID = target.ID
	completion.Actor.SessionID = "different-session"
	if err := f.db.CompleteLocalPasswordReset(ctx, completion); !errors.Is(err, identity.ErrInvalidPasswordReset) {
		t.Fatalf("reservation accepted a different session: %v", err)
	}
	completion.Actor = actor
	completion.Actor.AccountID = "different-actor"
	if err := f.db.CompleteLocalPasswordReset(ctx, completion); !errors.Is(err, identity.ErrInvalidPasswordReset) {
		t.Fatalf("reservation accepted a different actor: %v", err)
	}
	completion.Actor = actor
	if err := f.manager.Revoke(ctx, f.adminRun.Token); err != nil {
		t.Fatal(err)
	}
	if err := f.db.CompleteLocalPasswordReset(ctx, completion); !errors.Is(err, identity.ErrInvalidAdminSession) {
		t.Fatalf("reset accepted a revoked administrator session: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("denied reset revoked the target session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied || events[1].ActorID != f.admin.ID {
		t.Fatalf("revoked-admin reset audit count=%d err=%v", len(events), err)
	}
}

func TestPasswordResetRechecksAuthorizationAfterReservation(t *testing.T) {
	for _, tt := range []struct {
		name string
		want error
	}{
		{"disabled", identity.ErrInvalidAdminSession},
		{"expired", identity.ErrInvalidAdminSession},
		{"demoted", identity.ErrGlobalAdminRequired},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
			secondAdmin, secondSession := f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			at := time.Now().Add(time.Second)
			attempt := identity.LocalPasswordResetAttempt{Actor: actor, TargetID: target.ID, OperationID: "reset-operation", OccurredAt: at}
			if err := f.db.ReserveLocalPasswordReset(ctx, attempt); err != nil {
				t.Fatal(err)
			}
			switch tt.name {
			case "disabled":
				other := identity.LocalAdminActor{AccountID: secondAdmin.ID, SessionID: secondSession.Principal.SessionID}
				if _, err := f.change.Execute(ctx, other, f.admin.ID, false); err != nil {
					t.Fatal(err)
				}
			case "expired":
				_, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?",
					at.Add(time.Second).UnixNano(), actor.SessionID)
				if err != nil {
					t.Fatal(err)
				}
			case "demoted":
				if _, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", actor.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			hash, err := identity.HashLocalPassword(rand.Reader, "temporary reset passphrase")
			if err != nil {
				t.Fatal(err)
			}
			err = f.db.CompleteLocalPasswordReset(ctx, identity.LocalPasswordResetCompletion{
				Actor: actor, TargetID: target.ID, OperationID: attempt.OperationID, EventID: "reset-outcome",
				RevocationEventID: "reset-revocation", NewHash: hash, OccurredAt: at.Add(2 * time.Second),
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("administrator %s after reservation: %v", tt.name, err)
			}
			if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
				t.Fatalf("denied reset affected target session: %v", err)
			}
			stored, err := f.db.FindLocalAccount(ctx, target.Username)
			if err != nil || stored.CredentialVersion != 1 || stored.MustChangePassword {
				t.Fatalf("denied reset affected target account: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied {
				t.Fatalf("authorization-change audit count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestAdministratorCanResetOwnPasswordAndIsSignedOut(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reset.Execute(ctx, actor, f.admin.ID, "new temporary passphrase"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("self-reset did not revoke administrator session: %v", err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := authenticator.Execute(ctx, f.admin.Username, "new temporary passphrase", netip.MustParseAddr("192.0.2.1"))
	if err != nil || !account.Enabled || !account.MustChangePassword {
		t.Fatalf("self-reset did not retain an enabled administrator requiring a password change: %v", err)
	}
	credentials, err := f.manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.CheckSessionAccess(credentials.Principal, session.AccessNormal); !errors.Is(err, session.ErrPasswordChangeRequired) {
		t.Fatalf("self-reset temporary session reached normal actions: %v", err)
	}
}

func TestPasswordResetDoesNotEnableDisabledAccount(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.change.Execute(ctx, actor, target.ID, false); err != nil {
		t.Fatal(err)
	}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase"); err != nil {
		t.Fatal(err)
	}
	stored, err := f.db.FindLocalAccount(ctx, target.Username)
	if err != nil || stored.Enabled || !stored.MustChangePassword || stored.CredentialVersion != 3 {
		t.Fatalf("reset changed disabled account state: %v", err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	client := netip.MustParseAddr("192.0.2.2")
	if _, err := authenticator.Execute(ctx, target.Username, "temporary reset passphrase", client); !errors.Is(err, identity.ErrAuthenticationFailed) {
		t.Fatalf("disabled reset account authenticated: %v", err)
	}
	if _, err := f.change.Execute(ctx, actor, target.ID, true); err != nil {
		t.Fatal(err)
	}
	account, err := authenticator.Execute(ctx, target.Username, "temporary reset passphrase", client)
	if err != nil || !account.MustChangePassword {
		t.Fatalf("reenabled account lost mandatory password change: %v", err)
	}
}

func TestPasswordResetDoesNotOverwriteConcurrentAccountStateChange(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	at := time.Now().Add(time.Second)
	if err := f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
		Actor: actor, TargetID: target.ID, OperationID: "reset-operation", OccurredAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.change.Execute(ctx, actor, target.ID, false); err != nil {
		t.Fatal(err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "temporary reset passphrase")
	if err != nil {
		t.Fatal(err)
	}
	err = f.db.CompleteLocalPasswordReset(ctx, identity.LocalPasswordResetCompletion{
		Actor: actor, TargetID: target.ID, OperationID: "reset-operation", EventID: "reset-outcome",
		RevocationEventID: "reset-revocation", NewHash: hash, OccurredAt: at.Add(time.Second),
	})
	if !errors.Is(err, identity.ErrLocalAccountChanged) {
		t.Fatalf("reset ignored concurrent account change: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, target.Username)
	if err != nil || stored.Enabled || stored.MustChangePassword || stored.CredentialVersion != 2 {
		t.Fatalf("concurrent disable was overwritten: %v", err)
	}
	matches, err := identity.VerifyLocalPassword("other strong passphrase", stored.PasswordHash)
	if err != nil || !matches {
		t.Fatalf("concurrent account change did not retain old password: %v", err)
	}
}

func TestPasswordResetRecordsHashingFailure(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	reset, err := identity.NewResetLocalPassword(f.db, io.LimitReader(rand.Reader, 48), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase")
	if !errors.Is(err, io.EOF) || result.OperationID == "" {
		t.Fatalf("hashing failure was not surfaced: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("failed hash revoked a target session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted ||
		events[1].Outcome != AuditFailed || events[1].OperationID != result.OperationID {
		t.Fatalf("hash failure audit count=%d err=%v", len(events), err)
	}
}

type cancelAfterPasswordResetReservation struct {
	identity.LocalPasswordResetStore
	cancel context.CancelFunc
}

func (s cancelAfterPasswordResetReservation) ReserveLocalPasswordReset(ctx context.Context, attempt identity.LocalPasswordResetAttempt) error {
	if err := s.LocalPasswordResetStore.ReserveLocalPasswordReset(ctx, attempt); err != nil {
		return err
	}
	s.cancel()
	return nil
}

func TestPasswordResetSurfacesCanceledOutcomeAudit(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	store := cancelAfterPasswordResetReservation{LocalPasswordResetStore: f.db, cancel: cancel}
	reset, err := identity.NewResetLocalPassword(store, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase")
	if !errors.Is(err, context.Canceled) || !errors.Is(err, identity.ErrPasswordResetAudit) ||
		!strings.Contains(err.Error(), "record failed password reset") || result.OperationID == "" {
		t.Fatalf("canceled audit failure was not surfaced: %v", err)
	}
	if _, err := f.manager.Resolve(context.Background(), targetSession.Token); err != nil {
		t.Fatalf("canceled reset revoked the target session: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || events[0].OperationID != result.OperationID {
		t.Fatalf("canceled reset did not retain attempted audit: count=%d err=%v", len(events), err)
	}
}

func TestPasswordResetSurfacesValidationAuditFailure(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_reset_validation_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_password_reset' AND NEW.outcome = 'validation_rejected'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := reset.Execute(ctx, actor, target.ID, "short")
	if !errors.Is(err, identity.ErrInvalidPassword) || !errors.Is(err, identity.ErrPasswordResetAudit) || result.OperationID == "" {
		t.Fatalf("validation audit failure was not surfaced: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("failed validation revoked target session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || events[0].OperationID != result.OperationID {
		t.Fatalf("validation audit failure did not retain attempted event: count=%d err=%v", len(events), err)
	}
}

func TestExpiredPasswordResetFailureIsAuditedWithoutReportingAuditOutage(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	start := time.Now().Add(time.Second)
	calls := 0
	clock := func() time.Time {
		calls++
		if calls == 1 {
			return start
		}
		return start.Add(passwordResetWindow)
	}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, clock)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := reset.Execute(ctx, actor, target.ID, "short")
	if !errors.Is(err, identity.ErrInvalidPassword) || !errors.Is(err, identity.ErrPasswordResetExpired) ||
		errors.Is(err, identity.ErrPasswordResetAudit) {
		t.Fatalf("expired rejected reset had an incorrect error: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied || events[1].OperationID != result.OperationID {
		t.Fatalf("expired reset was not audited: count=%d err=%v", len(events), err)
	}
}

func TestPasswordResetRejectsExpiryReplayAndConcurrentCredentialChanges(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	at := time.Now().Add(time.Second)
	for _, id := range []string{"reset-one", "reset-two", "reset-expired"} {
		if err := f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
			Actor: actor, TargetID: target.ID, OperationID: id, OccurredAt: at,
		}); err != nil {
			t.Fatal(err)
		}
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "temporary reset passphrase")
	if err != nil {
		t.Fatal(err)
	}
	completions := []identity.LocalPasswordResetCompletion{
		{Actor: actor, TargetID: target.ID, OperationID: "reset-one", EventID: "outcome-one", RevocationEventID: "revoke-one", NewHash: hash, OccurredAt: at.Add(time.Second)},
		{Actor: actor, TargetID: target.ID, OperationID: "reset-two", EventID: "outcome-two", RevocationEventID: "revoke-two", NewHash: hash, OccurredAt: at.Add(time.Second)},
	}
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, completion := range completions {
		group.Add(1)
		go func(completion identity.LocalPasswordResetCompletion) {
			defer group.Done()
			results <- f.db.CompleteLocalPasswordReset(ctx, completion)
		}(completion)
	}
	group.Wait()
	close(results)
	var successes, conflicts int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, identity.ErrLocalAccountChanged):
			conflicts++
		default:
			t.Errorf("unexpected concurrent reset result: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	completions[0].EventID, completions[0].RevocationEventID = "replay-result", "replay-revocation"
	if err := f.db.CompleteLocalPasswordReset(ctx, completions[0]); !errors.Is(err, identity.ErrInvalidPasswordReset) {
		t.Fatalf("completed reset was replayed: %v", err)
	}
	expired := identity.LocalPasswordResetCompletion{
		Actor: actor, TargetID: target.ID, OperationID: "reset-expired", EventID: "expired-result",
		RevocationEventID: "expired-revocation", NewHash: hash, OccurredAt: at.Add(passwordResetWindow),
	}
	if err := f.db.CompleteLocalPasswordReset(ctx, expired); !errors.Is(err, identity.ErrPasswordResetExpired) {
		t.Fatalf("expired reset reservation: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, target.Username)
	if err != nil || stored.CredentialVersion != 2 || !stored.MustChangePassword {
		t.Fatalf("concurrent reset state: version=%d mustChange=%t err=%v", stored.CredentialVersion, stored.MustChangePassword, err)
	}
}

func TestPasswordResetRollsBackWhenAuditFails(t *testing.T) {
	for _, tt := range []struct {
		name, condition string
	}{
		{"attempted", "NEW.action = 'console.local_password_reset' AND NEW.outcome = 'attempted'"},
		{"succeeded", "NEW.action = 'console.local_password_reset' AND NEW.outcome = 'succeeded'"},
		{"revocation", "NEW.action = 'console.sessions.revoke_on_password_reset'"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_reset_audit BEFORE INSERT ON audit_events
WHEN `+tt.condition+` BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			result, err := reset.Execute(ctx, actor, target.ID, "temporary reset passphrase")
			if err == nil || result.OperationID == "" {
				t.Fatal("password reset succeeded without durable audit")
			}
			stored, err := f.db.FindLocalAccount(ctx, target.Username)
			if err != nil || stored.MustChangePassword || stored.CredentialVersion != 1 {
				t.Fatalf("failed reset left account changes: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
				t.Fatalf("failed reset revoked target session: %v", err)
			}
		})
	}
}

func TestVersionFourMigrationPreservesAccountsAndSessions(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	path := f.db.path
	if _, err := f.db.db.ExecContext(ctx, "DROP TABLE cognito_session_credentials; DROP TABLE cognito_identities; ALTER TABLE console_sessions DROP COLUMN auth_source; DROP TABLE cognito_authorization_attempts; DROP TABLE local_account_creation_attempts; DROP TABLE password_reset_attempts; PRAGMA user_version = 4"); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(ctx, filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := manager.Resolve(ctx, f.adminRun.Token)
	if err != nil || principal.AccountID != f.admin.ID {
		t.Fatalf("version-four migration lost the administrator session: %v", err)
	}
	var tableCount int
	err = db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'password_reset_attempts'").Scan(&tableCount)
	if err != nil || tableCount != 1 {
		t.Fatalf("version-four migration did not create reset table: %v", err)
	}
}
