package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func accessListService(t *testing.T, db *Database) identity.ListAccessBindings {
	t.Helper()
	service, err := identity.NewListAccessBindings(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestAccessBindingsListSourcesScopesAndPages(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	f.addAccount(t, "id-alice", "alice", "cluster-a", "ADMIN")
	f.addAccount(t, "id-bob", "bob", "", "")
	if _, err := f.db.db.ExecContext(ctx, `INSERT INTO identities (id, enabled) VALUES ('id-cognito', 1);
INSERT INTO cognito_identities (identity_id, issuer, subject) VALUES ('id-cognito', 'https://issuer.example', 'opaque-subject');
INSERT INTO role_grants (account_id, cluster_id, role) VALUES ('id-alice', '', 'VIEWER'), ('id-cognito', 'cluster-b', 'OPERATOR');
INSERT INTO cognito_group_bindings (issuer, group_name, cluster_id, role) VALUES ('https://issuer.example', 'ops', 'cluster-a', 'OPERATOR')`); err != nil {
		t.Fatal(err)
	}
	actor := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	if _, err := roleGrantService(t, f.db).Execute(ctx, actor, "id-bob", "", authorization.RoleNone, authorization.RoleViewer); err != nil {
		t.Fatal(err)
	}
	list := accessListService(t, f.db)
	entries := map[string]identity.AccessIdentity{}
	var order []string
	cursor, pages := "", 0
	for {
		page, err := list.Execute(ctx, actor, cursor, 2)
		if err != nil || page.OperationID == "" || page.PolicyRevision != 1 || len(page.Identities) > 2 {
			t.Fatalf("page %d = %+v, %v", pages, page, err)
		}
		wantGroups := []identity.GroupGrant{{Issuer: "https://issuer.example", Group: "ops",
			Grant: authorization.Grant{ClusterID: "cluster-a", Role: authorization.RoleOperator}}}
		if !cmp.Equal(page.GroupGrants, wantGroups) || page.GroupGrantsTruncated {
			t.Fatalf("group grants: %s", cmp.Diff(wantGroups, page.GroupGrants))
		}
		for _, entry := range page.Identities {
			entries[entry.ID] = entry
			order = append(order, entry.ID)
		}
		pages++
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor != page.Identities[len(page.Identities)-1].ID {
			t.Fatalf("cursor %q is not the last listed identity", page.NextCursor)
		}
		cursor = page.NextCursor
	}
	if pages != 2 || len(order) != 4 || !slices.IsSorted(order) {
		t.Fatalf("paging returned %d pages in order %v", pages, order)
	}
	want := map[string]identity.AccessIdentity{
		"id-alice": {ID: "id-alice", Source: "local", Label: "alice", Enabled: true,
			Grants: []authorization.Grant{{Role: authorization.RoleViewer}, {ClusterID: "cluster-a", Role: authorization.RoleAdmin}}},
		"id-bob": {ID: "id-bob", Source: "local", Label: "bob", Enabled: true, Grants: []authorization.Grant{{Role: authorization.RoleViewer}}},
		"id-cognito": {ID: "id-cognito", Source: "cognito", Label: "opaque-subject", Enabled: true,
			Grants: []authorization.Grant{{ClusterID: "cluster-b", Role: authorization.RoleOperator}}},
		f.admin.ID: {ID: f.admin.ID, Source: "local", Label: "admin", Enabled: true, Grants: []authorization.Grant{{Role: authorization.RoleAdmin}}},
	}
	if !cmp.Equal(entries, want) {
		t.Fatalf("listed identities: %s", cmp.Diff(want, entries))
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.access.list", Limit: 10})
	if err != nil || len(events) != 2 || events[0].Outcome != AuditSucceeded || events[0].ActorID != f.admin.ID || events[0].PolicyRevision != 1 {
		t.Fatalf("listing audit = %+v, %v", events, err)
	}
}

func TestAccessBindingsListRejectsNonAdminsInvalidRequestsAndAuditFailure(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, aliceRun := f.addAccount(t, "id-alice", "alice", "cluster-a", "ADMIN")
	list := accessListService(t, f.db)
	clusterAdmin := identity.LocalAdminActor{AccountID: "id-alice", SessionID: aliceRun.Principal.SessionID}
	if page, err := list.Execute(ctx, clusterAdmin, "", 10); !errors.Is(err, identity.ErrGlobalAdminRequired) || len(page.Identities) != 0 {
		t.Fatalf("cluster ADMIN listing = %+v, %v", page, err)
	}
	admin := identity.LocalAdminActor{AccountID: f.admin.ID, SessionID: f.adminRun.Principal.SessionID}
	for _, tt := range []struct {
		after string
		limit int
	}{{"", 0}, {"", identity.MaxAccessPageSize + 1}, {" padded", 10}, {strings.Repeat("x", 129), 10}} {
		if _, err := list.Execute(ctx, admin, tt.after, tt.limit); !errors.Is(err, identity.ErrInvalidAccessList) {
			t.Fatalf("listing %+v error = %v", tt, err)
		}
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.access.list", Limit: 10})
	if err != nil || len(events) != 5 || events[0].Outcome != AuditDenied || events[1].Outcome != AuditValidationRejected {
		t.Fatalf("listing denial audit = %+v, %v", events, err)
	}
	if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_access_list_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.access.list' BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if page, err := list.Execute(ctx, admin, "", 10); !errors.Is(err, identity.ErrAccessListAudit) || len(page.Identities) != 0 {
		t.Fatalf("unaudited listing returned data: %+v, %v", page, err)
	}
}
