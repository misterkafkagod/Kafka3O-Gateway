package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestCreateLocalAccountRequiresPasswordChangeAndGrantsNoRoles(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	creator, err := identity.NewCreateLocalAccount(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	const temporaryPassword = "new temporary passphrase"
	result, err := creator.Execute(ctx, actor, "Sample-User", temporaryPassword)
	if err != nil || result.OperationID == "" || result.Account.ID == "" ||
		result.Account.Username != "sample-user" || result.Account.PasswordHash != "" ||
		!result.Account.Enabled || !result.Account.MustChangePassword || result.Account.CredentialVersion != 1 {
		t.Fatalf("local account creation result was invalid: %v", err)
	}
	stored, err := f.db.FindLocalAccount(ctx, "SAMPLE-user")
	if err != nil || stored.ID != result.Account.ID || !stored.Enabled || !stored.MustChangePassword {
		t.Fatalf("created local account was not persisted: %v", err)
	}
	matches, err := identity.VerifyLocalPassword(temporaryPassword, stored.PasswordHash)
	if err != nil || !matches || stored.PasswordHash == temporaryPassword {
		t.Fatalf("created account password was not safely hashed: %v", err)
	}
	grants, err := f.db.ListAccountGrants(ctx, result.Account.ID)
	if err != nil || len(grants) != 0 {
		t.Fatalf("new local account received implicit grants: count=%d err=%v", len(grants), err)
	}
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	client := netip.MustParseAddr("192.0.2.3")
	account, err := authenticator.Execute(ctx, "sample-user", temporaryPassword, client)
	if err != nil || !account.MustChangePassword {
		t.Fatalf("temporary account login failed: %v", err)
	}
	credentials, err := f.manager.IssueLocal(ctx, account)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.CheckSessionAccess(credentials.Principal, session.AccessNormal); !errors.Is(err, session.ErrPasswordChangeRequired) {
		t.Fatalf("new user could bypass mandatory password change: %v", err)
	}
	change, err := identity.NewChangeLocalPassword(authenticator, f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := change.Execute(ctx, account.ID, account.Username, temporaryPassword, "new permanent passphrase", client); err != nil {
		t.Fatal(err)
	}
	if _, err := f.manager.Resolve(ctx, credentials.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("password replacement did not revoke the temporary session: %v", err)
	}
	account, err = authenticator.Execute(ctx, "sample-user", "new permanent passphrase", client)
	if err != nil || account.MustChangePassword {
		t.Fatalf("created account could not authenticate after password change: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted || events[1].Outcome != AuditSucceeded ||
		events[0].EventID != result.OperationID || events[1].OperationID != result.OperationID ||
		events[1].ActorID != f.admin.ID || events[1].ResourceID != result.Account.ID {
		t.Fatalf("local creation audit count=%d err=%v", len(events), err)
	}
	all, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	serialized := fmt.Sprintf("%+v", all)
	for _, secret := range []string{"Sample-User", "sample-user", temporaryPassword, "new permanent passphrase", credentials.Token, credentials.CSRFToken} {
		if strings.Contains(serialized, secret) {
			t.Fatal("local account creation audit contained a username or credential")
		}
	}
}

func TestLocalAccountCreationRequiresCurrentGlobalAdmin(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	for _, tt := range []struct {
		name  string
		actor identity.LocalAdminActor
		want  error
	}{
		{"cluster admin", identity.LocalAdminActor{AccountID: clusterAdmin.ID, SessionID: clusterSession.Principal.SessionID}, identity.ErrGlobalAdminRequired},
		{"invalid session", identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: "invalid-session"}, identity.ErrInvalidAdminSession},
	} {
		t.Run(tt.name, func(t *testing.T) {
			creator, err := identity.NewCreateLocalAccount(f.db, io.LimitReader(rand.Reader, 48), time.Now)
			if err != nil {
				t.Fatal(err)
			}
			result, err := creator.Execute(ctx, tt.actor, "not-created", "temporary passphrase")
			if !errors.Is(err, tt.want) || result.OperationID == "" || result.Account.ID != "" {
				t.Fatalf("unauthorized account creation: %v", err)
			}
		})
	}
	if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", f.admin.ID); err != nil {
		t.Fatal(err)
	}
	creator, err := identity.NewCreateLocalAccount(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := creator.Execute(ctx, actor, "not-created", "temporary passphrase"); !errors.Is(err, identity.ErrAdminPasswordChange) {
		t.Fatalf("restricted administrator created an account: %v", err)
	}
	if _, err := f.db.FindLocalAccount(ctx, "not-created"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("unauthorized request created an account: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 3 || events[0].ActorID != clusterAdmin.ID || events[0].Outcome != AuditDenied ||
		events[1].ActorID != "anonymous" || events[1].Outcome != AuditDenied || events[2].ActorID != f.admin.ID {
		t.Fatalf("unauthorized creation audit count=%d err=%v", len(events), err)
	}
	var attempts int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM local_account_creation_attempts").Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("unauthorized creation reserved a slot: count=%d err=%v", attempts, err)
	}
}

func TestLocalAccountCreationRejectsAndRedactsInvalidInput(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	creator, err := identity.NewCreateLocalAccount(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	for _, tt := range []struct {
		username, password string
		want               error
	}{
		{"invalid submitted username", "temporary passphrase", identity.ErrInvalidUsername},
		{"not-created", "bad!", identity.ErrInvalidPassword},
	} {
		result, err := creator.Execute(ctx, actor, tt.username, tt.password)
		if !errors.Is(err, tt.want) || result.OperationID == "" || result.Account.ID != "" {
			t.Fatalf("invalid account creation was not rejected: %v", err)
		}
	}
	if _, err := f.db.FindLocalAccount(ctx, "not-created"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("invalid input created a local account: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 4 {
		t.Fatalf("invalid account audit count=%d err=%v", len(events), err)
	}
	for i := 0; i < len(events); i += 2 {
		if events[i].Outcome != AuditAttempted || events[i+1].Outcome != AuditValidationRejected ||
			events[i].EventID != events[i+1].OperationID {
			t.Fatal("invalid account creation was not correlated")
		}
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, secret := range []string{"invalid submitted username", "not-created", "temporary passphrase", "bad!"} {
		if strings.Contains(serialized, secret) {
			t.Fatal("invalid account creation audit contained input")
		}
	}
}

func TestConcurrentLocalAccountCreationRejectsDuplicateNormalizedUsernames(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	creator, err := identity.NewCreateLocalAccount(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	type outcome struct {
		result identity.LocalAccountCreationResult
		err    error
	}
	results := make(chan outcome, 2)
	var group sync.WaitGroup
	for _, name := range []string{"Concurrent-User", "concurrent-user"} {
		group.Add(1)
		go func(name string) {
			defer group.Done()
			result, err := creator.Execute(ctx, actor, name, "temporary passphrase")
			results <- outcome{result, err}
		}(name)
	}
	group.Wait()
	close(results)
	var successes, conflicts int
	for outcome := range results {
		switch {
		case outcome.err == nil && outcome.result.Account.ID != "":
			successes++
		case errors.Is(outcome.err, identity.ErrUsernameTaken) && outcome.result.Account.ID == "":
			conflicts++
		default:
			t.Errorf("unexpected concurrent local account creation result: %v", outcome.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	var count int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM local_accounts WHERE username = 'concurrent-user'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate normalized usernames persisted: count=%d err=%v", count, err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 4 {
		t.Fatalf("concurrent creation audit count=%d err=%v", len(events), err)
	}
	var succeeded, rejected int
	for _, event := range events {
		if event.Outcome == AuditSucceeded {
			succeeded++
		}
		if event.Outcome == AuditValidationRejected {
			rejected++
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("concurrent creation outcomes: succeeded=%d rejected=%d", succeeded, rejected)
	}
}

func TestLocalAccountCreationRechecksAuthorizationAfterReservation(t *testing.T) {
	for _, tt := range []struct {
		name string
		want error
	}{
		{"disabled", identity.ErrInvalidAdminSession},
		{"expired", identity.ErrInvalidAdminSession},
		{"demoted", identity.ErrGlobalAdminRequired},
		{"temporary password", identity.ErrAdminPasswordChange},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			secondAdmin, secondSession := f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			at := time.Now().Add(time.Second)
			attempt := identity.LocalAccountCreationAttempt{Actor: actor, AccountID: "new-account", OperationID: "create-operation", OccurredAt: at}
			if err := f.db.ReserveLocalAccountCreation(ctx, attempt); err != nil {
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
			case "temporary password":
				if _, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", actor.AccountID); err != nil {
					t.Fatal(err)
				}
			}
			hash, err := identity.HashLocalPassword(rand.Reader, "temporary passphrase")
			if err != nil {
				t.Fatal(err)
			}
			completion := identity.LocalAccountCreationCompletion{
				Actor: actor, Account: identity.LocalAccount{ID: attempt.AccountID, Username: "new-user", PasswordHash: hash,
					Enabled: true, MustChangePassword: true, CredentialVersion: 1},
				OperationID: attempt.OperationID, EventID: "create-outcome", OccurredAt: at.Add(2 * time.Second),
			}
			if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, tt.want) {
				t.Fatalf("creation accepted administrator %s after reservation: %v", tt.name, err)
			}
			if _, err := f.db.FindLocalAccount(ctx, "new-user"); !errors.Is(err, identity.ErrAccountNotFound) {
				t.Fatalf("denied creation persisted a local account: %v", err)
			}
			completion.EventID = "replay-result"
			if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, identity.ErrInvalidAccountCreation) {
				t.Fatalf("denied creation could be replayed: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied || events[1].ActorID != f.admin.ID {
				t.Fatalf("authorization-change audit count=%d err=%v", len(events), err)
			}
		})
	}
}

func TestLocalAccountCreationBindsReservationAndRejectsExpiryAndReplay(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	at := time.Now().Add(time.Second)
	for _, attempt := range []identity.LocalAccountCreationAttempt{
		{Actor: actor, AccountID: "bound-account", OperationID: "create-bound", OccurredAt: at},
		{Actor: actor, AccountID: "expired-account", OperationID: "create-expired", OccurredAt: at},
	} {
		if err := f.db.ReserveLocalAccountCreation(ctx, attempt); err != nil {
			t.Fatal(err)
		}
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "temporary passphrase")
	if err != nil {
		t.Fatal(err)
	}
	completion := identity.LocalAccountCreationCompletion{
		Actor: actor, Account: identity.LocalAccount{ID: "bound-account", Username: "bound-user", PasswordHash: hash,
			Enabled: true, MustChangePassword: true, CredentialVersion: 1},
		OperationID: "create-bound", EventID: "bound-result", OccurredAt: at.Add(time.Second),
	}
	completion.Account.ID = "other-account"
	if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, identity.ErrInvalidAccountCreation) {
		t.Fatalf("creation reservation accepted a different account: %v", err)
	}
	completion.Account.ID = "bound-account"
	completion.Actor.SessionID = "other-session"
	if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, identity.ErrInvalidAccountCreation) {
		t.Fatalf("creation reservation accepted a different session: %v", err)
	}
	completion.Actor = actor
	completion.Actor.AccountID = "other-actor"
	if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, identity.ErrInvalidAccountCreation) {
		t.Fatalf("creation reservation accepted a different actor: %v", err)
	}
	completion.Actor = actor
	if err := f.db.CompleteLocalAccountCreation(ctx, completion); err != nil {
		t.Fatal(err)
	}
	completion.EventID = "bound-replay"
	if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, identity.ErrInvalidAccountCreation) {
		t.Fatalf("completed account creation could be replayed: %v", err)
	}
	completion.Account.ID, completion.Account.Username = "expired-account", "expired-user"
	completion.OperationID, completion.EventID, completion.OccurredAt = "create-expired", "expired-result", at.Add(localAccountCreationWindow)
	if err := f.db.CompleteLocalAccountCreation(ctx, completion); !errors.Is(err, identity.ErrAccountCreationExpired) {
		t.Fatalf("expired account creation could complete: %v", err)
	}
	if _, err := f.db.FindLocalAccount(ctx, "expired-user"); !errors.Is(err, identity.ErrAccountNotFound) {
		t.Fatalf("expired account creation persisted: %v", err)
	}
}

func TestLocalAccountCreationRollsBackWhenAuditFails(t *testing.T) {
	for _, tt := range []struct {
		name, condition, username string
		wantAudit                 bool
	}{
		{"attempted", "NEW.outcome = 'attempted'", "new-user", false},
		{"succeeded", "NEW.outcome = 'succeeded'", "new-user", false},
		{"invalid input", "NEW.outcome = 'validation_rejected'", "invalid username", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_create_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_account.create' AND `+tt.condition+`
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			creator, err := identity.NewCreateLocalAccount(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
			result, err := creator.Execute(ctx, actor, tt.username, "temporary passphrase")
			if err == nil || result.OperationID == "" || result.Account.ID != "" {
				t.Fatal("account creation succeeded without durable audit")
			}
			if tt.wantAudit && (!errors.Is(err, identity.ErrAccountCreationAudit) || !errors.Is(err, identity.ErrInvalidUsername)) {
				t.Fatalf("validation audit failure was hidden: %v", err)
			}
			if _, err := f.db.FindLocalAccount(ctx, "new-user"); !errors.Is(err, identity.ErrAccountNotFound) {
				t.Fatalf("unaudited account was persisted: %v", err)
			}
		})
	}
}

func TestLocalAccountCreationRecordsHashFailure(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	creator, err := identity.NewCreateLocalAccount(f.db, io.LimitReader(rand.Reader, 48), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := creator.Execute(ctx, actor, "new-user", "temporary passphrase")
	if !errors.Is(err, io.EOF) || result.OperationID == "" || result.Account.ID != "" {
		t.Fatalf("creation hash failure was not surfaced: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditAttempted ||
		events[1].Outcome != AuditFailed || events[1].OperationID != result.OperationID {
		t.Fatalf("creation hash failure audit count=%d err=%v", len(events), err)
	}
}

type cancelAfterLocalAccountCreationReservation struct {
	identity.LocalAccountCreationStore
	cancel context.CancelFunc
}

func (s cancelAfterLocalAccountCreationReservation) ReserveLocalAccountCreation(ctx context.Context, attempt identity.LocalAccountCreationAttempt) error {
	if err := s.LocalAccountCreationStore.ReserveLocalAccountCreation(ctx, attempt); err != nil {
		return err
	}
	s.cancel()
	return nil
}

func TestLocalAccountCreationSurfacesCanceledOutcomeAudit(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	creator, err := identity.NewCreateLocalAccount(cancelAfterLocalAccountCreationReservation{f.db, cancel}, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := creator.Execute(ctx, actor, "new-user", "temporary passphrase")
	if !errors.Is(err, context.Canceled) || !errors.Is(err, identity.ErrAccountCreationAudit) || result.OperationID == "" {
		t.Fatalf("canceled creation audit failure was not surfaced: %v", err)
	}
	events, err := f.db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 1 || events[0].Outcome != AuditAttempted || events[0].OperationID != result.OperationID {
		t.Fatalf("canceled creation did not retain attempted event: count=%d err=%v", len(events), err)
	}
}

func TestExpiredCreationFailureIsAuditedWithoutReportingAuditOutage(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	start := time.Now().Add(time.Second)
	calls := 0
	clock := func() time.Time {
		calls++
		if calls == 1 {
			return start
		}
		return start.Add(localAccountCreationWindow)
	}
	creator, err := identity.NewCreateLocalAccount(f.db, rand.Reader, clock)
	if err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	result, err := creator.Execute(ctx, actor, "invalid username", "temporary passphrase")
	if !errors.Is(err, identity.ErrInvalidUsername) || !errors.Is(err, identity.ErrAccountCreationExpired) ||
		errors.Is(err, identity.ErrAccountCreationAudit) {
		t.Fatalf("expired rejected creation had an incorrect error: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.create", Limit: 10})
	if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied || events[1].OperationID != result.OperationID {
		t.Fatalf("expired creation was not audited: count=%d err=%v", len(events), err)
	}
}

func TestVersionFiveMigrationPreservesAccountsSessionsAndResetReservations(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, _ := f.addAccount(t, "subject-1", "other-user", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	at := time.Now().Add(time.Second)
	if err := f.db.ReserveLocalPasswordReset(ctx, identity.LocalPasswordResetAttempt{
		Actor: actor, TargetID: target.ID, OperationID: "reset-before-migration", OccurredAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	path := f.db.path
	if _, err := f.db.db.ExecContext(ctx, "DROP TABLE console_browser_sessions; ALTER TABLE console_sessions DROP COLUMN browser_binding_hash; DROP TABLE cognito_group_bindings; DROP TABLE cognito_session_credentials; DROP TABLE cognito_identities; ALTER TABLE console_sessions DROP COLUMN auth_source; DROP TABLE cognito_authorization_attempts; DROP TABLE local_account_creation_attempts; "+dropSchemaAfterVersionTen+"PRAGMA user_version = 5"); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDatabase(ctx, path)
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
		t.Fatalf("version-five migration lost the administrator session: %v", err)
	}
	hash, err := identity.HashLocalPassword(rand.Reader, "reset temporary passphrase")
	if err != nil {
		t.Fatal(err)
	}
	err = db.CompleteLocalPasswordReset(ctx, identity.LocalPasswordResetCompletion{
		Actor: actor, TargetID: target.ID, OperationID: "reset-before-migration", EventID: "reset-after-migration",
		RevocationEventID: "revoke-after-migration", NewHash: hash, OccurredAt: at.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("version-five migration lost the pending reset: %v", err)
	}
	var version, tables int
	if err := db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 13 {
		t.Fatalf("version-five migration schema version=%d err=%v", version, err)
	}
	err = db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'local_account_creation_attempts'").Scan(&tables)
	if err != nil || tables != 1 {
		t.Fatalf("version-five migration did not create reservation table: %v", err)
	}
}
