package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func roleGrantService(t *testing.T, db *Database) identity.ChangeRoleGrant {
	t.Helper()
	service, err := identity.NewChangeRoleGrant(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func roleGrantEvents(t *testing.T, db *Database) []AuditEvent {
	t.Helper()
	events, err := db.ListAuditEvents(context.Background(), AuditFilter{Global: true, Action: "console.role_grant.change", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func TestRoleGrantChangesApplyToNextResolutionAndAuditBeforeAfter(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, alice := f.addAccount(t, "alice", "alice", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	grants := roleGrantService(t, f.db)

	first, err := grants.Execute(ctx, actor, "alice", "", authorization.RoleNone, authorization.RoleViewer)
	if err != nil || first.PolicyRevision != 1 || first.OperationID == "" {
		t.Fatalf("global grant = %+v, %v", first, err)
	}
	second, err := grants.Execute(ctx, actor, "alice", "cluster-a", authorization.RoleNone, authorization.RoleAdmin)
	if err != nil || second.PolicyRevision != 2 {
		t.Fatalf("cluster grant = %+v, %v", second, err)
	}
	_, resolved, err := f.manager.ResolveGrants(ctx, alice.Token, "cluster-a")
	want := []authorization.Grant{{Role: authorization.RoleViewer}, {ClusterID: "cluster-a", Role: authorization.RoleAdmin}}
	if err != nil || !cmp.Equal(resolved, want) {
		t.Fatalf("grants after change = %+v, %v", resolved, err)
	}

	if _, err := grants.Execute(ctx, actor, "alice", "cluster-a", authorization.RoleAdmin, authorization.RoleNone); err != nil {
		t.Fatal(err)
	}
	_, resolved, err = f.manager.ResolveGrants(ctx, alice.Token, "cluster-a")
	if err != nil || !cmp.Equal(resolved, []authorization.Grant{{Role: authorization.RoleViewer}}) {
		t.Fatalf("existing session kept a removed grant: %+v, %v", resolved, err)
	}

	unchanged, err := grants.Execute(ctx, actor, "alice", "", authorization.RoleViewer, authorization.RoleViewer)
	if err != nil || unchanged.PolicyRevision != 3 {
		t.Fatalf("idempotent change advanced the revision: %+v, %v", unchanged, err)
	}

	events := roleGrantEvents(t, f.db)
	var outcomes [][3]string
	for _, event := range events {
		if event.Outcome == AuditSucceeded {
			if event.ActorID != f.admin.ID || event.ResourceID != "alice" {
				t.Fatalf("event attribution = %+v", event)
			}
			outcomes = append(outcomes, [3]string{event.ClusterID, event.BeforeState, event.AfterState})
		}
	}
	wantOutcomes := [][3]string{{"", "", "VIEWER"}, {"cluster-a", "", "ADMIN"}, {"cluster-a", "ADMIN", ""}, {"", "VIEWER", "VIEWER"}}
	if !cmp.Equal(outcomes, wantOutcomes) {
		t.Fatalf("audited before/after = %v, want %v", outcomes, wantOutcomes)
	}
}

func TestRoleGrantChangesDenyClusterAdminsSelfChangesAndConflicts(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	f.addAccount(t, "bob", "bob", "", "")
	_, aliceRun := f.addAccount(t, "alice", "alice", "cluster-a", "ADMIN")
	admin := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	clusterAdmin := identity.LocalAdminActor{AccountID: "alice", SessionID: aliceRun.Principal.SessionID}
	grants := roleGrantService(t, f.db)

	for _, tt := range []struct {
		name    string
		actor   identity.LocalAdminActor
		target  string
		cluster string
		want    error
		outcome AuditOutcome
	}{
		{"cluster admin grants on its cluster", clusterAdmin, "bob", "cluster-a", identity.ErrGlobalAdminRequired, AuditDenied},
		{"cluster admin grants globally", clusterAdmin, "bob", "", identity.ErrGlobalAdminRequired, AuditDenied},
		{"cluster admin escalates itself", clusterAdmin, "alice", "", identity.ErrGlobalAdminRequired, AuditDenied},
		{"admin changes itself", admin, f.admin.ID, "", identity.ErrSelfAccessChange, AuditDenied},
		{"unknown identity", admin, "nobody", "", identity.ErrAccessTargetNotFound, AuditValidationRejected},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := len(roleGrantEvents(t, f.db))
			expected := authorization.RoleNone
			if tt.target == f.admin.ID {
				expected = authorization.RoleAdmin
			}
			if _, err := grants.Execute(ctx, tt.actor, tt.target, tt.cluster, expected, authorization.RoleAdmin); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			events := roleGrantEvents(t, f.db)
			if len(events) != before+2 || events[len(events)-1].Outcome != tt.outcome || events[len(events)-1].ActorID != tt.actor.AccountID {
				t.Fatalf("denial audit = %+v", events[before:])
			}
		})
	}

	if _, err := grants.Execute(ctx, admin, "bob", "", authorization.RoleViewer, authorization.RoleAdmin); !errors.Is(err, identity.ErrPolicyConflict) {
		t.Fatalf("stale expected role error = %v", err)
	}
	var grantsCount, revision int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM role_grants WHERE account_id IN ('alice', 'bob')").Scan(&grantsCount); err != nil || grantsCount != 1 {
		t.Fatalf("denied changes wrote grants: count=%d err=%v", grantsCount, err)
	}
	if err := f.db.db.QueryRowContext(ctx, "SELECT revision FROM policy_state").Scan(&revision); err != nil || revision != 0 {
		t.Fatalf("denied changes advanced the revision: %d %v", revision, err)
	}
	var globalAdmins int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM role_grants WHERE cluster_id = '' AND role = 'ADMIN'").Scan(&globalAdmins); err != nil || globalAdmins != 1 {
		t.Fatalf("the only local global ADMIN was demoted: %d %v", globalAdmins, err)
	}
}

func TestRoleGrantChangeAuditFailureBlocksTheChange(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	f.addAccount(t, "alice", "alice", "", "")
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_role_grant_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.role_grant.change' AND NEW.outcome = 'succeeded' BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := roleGrantService(t, f.db).Execute(ctx, actor, "alice", "", authorization.RoleNone, authorization.RoleViewer); !errors.Is(err, identity.ErrAccessChangeAudit) {
		t.Fatalf("audit failure error = %v", err)
	}
	var count, revision int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM role_grants WHERE account_id = 'alice'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("unaudited grant was written: %d %v", count, err)
	}
	if err := f.db.db.QueryRowContext(ctx, "SELECT revision FROM policy_state").Scan(&revision); err != nil || revision != 0 {
		t.Fatalf("unaudited change advanced the revision: %d %v", revision, err)
	}
}

func TestRoleGrantChangeRejectsInvalidInputBeforeStorage(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	grants := roleGrantService(t, f.db)
	before := len(roleGrantEvents(t, f.db))
	for _, tt := range []struct {
		target, cluster string
		role            authorization.Role
	}{{"", "", authorization.RoleViewer}, {"alice", "cluster a", authorization.RoleViewer}, {"alice", "", "OWNER"}} {
		if _, err := grants.Execute(context.Background(), actor, tt.target, tt.cluster, authorization.RoleNone, tt.role); !errors.Is(err, identity.ErrInvalidAccessChange) {
			t.Fatalf("%+v error = %v", tt, err)
		}
	}
	if after := len(roleGrantEvents(t, f.db)); after != before {
		t.Fatalf("invalid input reached storage: %d events", after-before)
	}
}
