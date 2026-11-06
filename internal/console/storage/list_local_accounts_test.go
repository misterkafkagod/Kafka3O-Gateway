package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func TestListLocalAccountsPaginatesAndAuditsWithoutCredentials(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	alice, aliceSession := f.addAccount(t, "alice-id", "alice", "", "")
	bob, _ := f.addAccount(t, "bob-id", "bob", "", "")
	charlie, _ := f.addAccount(t, "charlie-id", "charlie", "", "")
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO identities (id, enabled) VALUES ('non-local-id', 1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", bob.ID); err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.change.Execute(ctx, actor, charlie.ID, false); err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	first, err := list.Execute(ctx, actor, "", 2)
	if err != nil || first.OperationID == "" || len(first.Accounts) != 2 || first.Accounts[0].Username != "admin" ||
		first.Accounts[1].ID != alice.ID || first.NextCursor != alice.ID {
		t.Fatalf("first account page was invalid: %v", err)
	}
	second, err := list.Execute(ctx, actor, first.NextCursor, 2)
	if err != nil || second.OperationID == "" || second.OperationID == first.OperationID || len(second.Accounts) != 2 ||
		second.Accounts[0].ID != bob.ID || !second.Accounts[0].MustChangePassword ||
		second.Accounts[1].ID != charlie.ID || second.Accounts[1].Enabled || second.NextCursor != "" {
		t.Fatalf("second account page was invalid: %v", err)
	}
	afterEnd, err := list.Execute(ctx, actor, charlie.ID, 2)
	if err != nil || afterEnd.Accounts == nil || len(afterEnd.Accounts) != 0 || afterEnd.NextCursor != "" {
		t.Fatalf("empty account page was invalid: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, aliceSession.Token); err != nil {
		t.Fatalf("listing revoked a target session: %v", err)
	}
	encoded, err := json.Marshal([]identity.LocalAccountListResult{first, second})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"passwordHash", "credentialVersion", "other strong passphrase", aliceSession.Token, aliceSession.CSRFToken} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("account listing exposed a credential")
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
	if err != nil || len(events) != 3 {
		t.Fatalf("account listing audit count=%d err=%v", len(events), err)
	}
	for i, operation := range []string{first.OperationID, second.OperationID, afterEnd.OperationID} {
		if events[i].OperationID != operation || events[i].ActorID != f.admin.ID || events[i].Outcome != AuditSucceeded ||
			events[i].ResourceID != "" || events[i].ResourceKind != "" {
			t.Fatal("account listing audit was not correlated or contained a cursor")
		}
	}
	for _, value := range []string{"alice", "bob", "charlie", "alice-id", aliceSession.Token, aliceSession.CSRFToken} {
		if strings.Contains(fmt.Sprintf("%+v", events), value) {
			t.Fatal("account listing audit contained a username, cursor, or credential")
		}
	}
}

func TestListLocalAccountsRequiresCurrentGlobalAdmin(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		actor identity.LocalAdminActor
		want  error
	}{
		{"cluster administrator", identity.LocalAdminActor{AccountID: clusterAdmin.ID, SessionID: clusterSession.Principal.SessionID}, identity.ErrGlobalAdminRequired},
		{"invalid session", identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: "invalid-session"}, identity.ErrInvalidAdminSession},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := list.Execute(ctx, tt.actor, "", 10)
			if !errors.Is(err, tt.want) || result.OperationID == "" || result.Accounts != nil {
				t.Fatalf("unauthorized listing returned account data: %v", err)
			}
		})
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if result, err := list.Execute(ctx, actor, "", 10); !errors.Is(err, identity.ErrAdminPasswordChange) || result.Accounts != nil {
		t.Fatalf("restricted administrator could list accounts: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
	if err != nil || len(events) != 3 || events[0].ActorID != clusterAdmin.ID || events[0].Outcome != AuditDenied ||
		events[1].ActorID != "anonymous" || events[1].Outcome != AuditDenied || events[2].ActorID != f.admin.ID {
		t.Fatalf("unauthorized listing audit count=%d err=%v", len(events), err)
	}
}

func TestListLocalAccountsRejectsAndRedactsInvalidPagination(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	for _, tt := range []struct {
		after string
		limit int
		want  error
	}{
		{"", 0, identity.ErrInvalidAccountList},
		{"", identity.MaxLocalAccountPageSize + 1, identity.ErrInvalidAccountList},
		{"unknown-submitted-cursor", 10, identity.ErrInvalidAccountCursor},
		{strings.Repeat("x", 129), 10, identity.ErrInvalidAccountCursor},
		{" leading-space", 10, identity.ErrInvalidAccountCursor},
		{"control\x00cursor", 10, identity.ErrInvalidAccountCursor},
	} {
		result, err := list.Execute(ctx, actor, tt.after, tt.limit)
		if !errors.Is(err, tt.want) || result.OperationID == "" || result.Accounts != nil {
			t.Fatalf("invalid pagination returned account data: %v", err)
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
	if err != nil || len(events) != 6 {
		t.Fatalf("invalid pagination audit count=%d err=%v", len(events), err)
	}
	for _, event := range events {
		if event.Outcome != AuditValidationRejected || event.ResourceID != "" || event.ActorID != f.admin.ID {
			t.Fatal("invalid pagination was not safely audited")
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", events), "unknown-submitted-cursor") {
		t.Fatal("account listing audit contained an untrusted cursor")
	}
}

func TestQueuedAccountListingRechecksAdministratorExpiry(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	start := time.Now().UTC().Add(time.Second)
	var current atomic.Int64
	current.Store(start.UnixNano())
	f.db.now = func() time.Time { return time.Unix(0, current.Load()).UTC() }
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = ? WHERE id = ?",
		start.Add(time.Minute).UnixNano(), actor.SessionID); err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListLocalAccounts(f.db, rand.Reader, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	var result identity.LocalAccountListResult
	err = runQueuedDatabaseOperation(t, f.db, func() {
		current.Store(start.Add(time.Minute).UnixNano())
	}, func() error {
		var err error
		result, err = list.Execute(ctx, actor, "", 10)
		return err
	})
	if !errors.Is(err, identity.ErrInvalidAdminSession) || result.Accounts != nil {
		t.Fatalf("queued expired administrator listed accounts: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied || events[0].ActorID != "anonymous" ||
		!events[0].OccurredAt.Equal(start.Add(time.Minute)) {
		t.Fatalf("queued expiry denial audit count=%d err=%v", len(events), err)
	}
}

func TestListLocalAccountsFailsClosedWhenAuditUnavailable(t *testing.T) {
	for _, deny := range []bool{false, true} {
		t.Run(fmt.Sprint(deny), func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_account_list_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_account.list'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			if deny {
				actor.SessionID = "invalid-session"
			}
			result, err := list.Execute(ctx, actor, "", 10)
			if !errors.Is(err, identity.ErrAccountListAudit) || result.OperationID == "" || result.Accounts != nil {
				t.Fatalf("unaudited listing returned account data or a denial: %v", err)
			}
		})
	}
}

func TestAccountListingAuditCommitFailureDoesNotReturnAccounts(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, err := f.db.db.ExecContext(ctx, `CREATE TABLE list_commit_failure (
identity_id TEXT REFERENCES identities(id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER fail_account_list_commit AFTER INSERT ON audit_events
WHEN NEW.action = 'console.local_account.list'
BEGIN INSERT INTO list_commit_failure (identity_id) VALUES ('missing-identity'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := list.Execute(ctx, actor, "", 10)
	if !errors.Is(err, identity.ErrAccountListAudit) || result.OperationID == "" || result.Accounts != nil {
		t.Fatalf("failed audit commit returned account data: %v", err)
	}
	if err := f.db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("failed list audit affected the administrator: %v", err)
	}
}
