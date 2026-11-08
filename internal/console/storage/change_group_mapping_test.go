package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

// groupMappingAdmin adds an enabled local account with the given global role and a local session for it.
func groupMappingAdmin(t *testing.T, f *refreshingSessionFixture, id string, role authorization.Role) identity.LocalAdminActor {
	t.Helper()
	ctx := context.Background()
	hash, err := identity.HashLocalPassword(rand.Reader, "administrator passphrase")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO identities (id, enabled) VALUES (?, 1)", id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, `INSERT INTO local_accounts
(identity_id, username, password_hash, must_change_password, credential_version) VALUES (?, ?, ?, 0, 1)`, id, id, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, '', ?)", id, role); err != nil {
		t.Fatal(err)
	}
	credentials, err := f.sessions.IssueLocal(ctx, identity.LocalAccount{ID: id, Username: id, Enabled: true, CredentialVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	return identity.LocalAdminActor{AccountID: id, SessionID: credentials.Principal.SessionID}
}

func groupMappingService(t *testing.T, f *refreshingSessionFixture) identity.ChangeGroupMapping {
	t.Helper()
	service, err := identity.NewChangeGroupMapping(f.db, rand.Reader, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestGroupMappingChangesApplyToTheNextCognitoResolution(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	admin := groupMappingAdmin(t, f, "admin", authorization.RoleAdmin)
	mappings := groupMappingService(t, f)
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone {
		t.Fatalf("unmapped Cognito session had role %q", role)
	}
	result, err := mappings.Execute(ctx, admin, f.provider.Issuer, "unmapped-group", "cluster-a", authorization.RoleNone, authorization.RoleOperator)
	if err != nil || result.PolicyRevision != 1 {
		t.Fatalf("map group = %+v, %v", result, err)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleOperator {
		t.Fatalf("mapped role = %q, want OPERATOR", role)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-b"); role != authorization.RoleNone {
		t.Fatalf("cluster mapping escaped its scope: %q", role)
	}
	if _, err := mappings.Execute(ctx, admin, f.provider.Issuer, "unmapped-group", "cluster-a", authorization.RoleOperator, authorization.RoleNone); err != nil {
		t.Fatal(err)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone {
		t.Fatalf("removed mapping stayed effective: %q", role)
	}
	if _, err := mappings.Execute(ctx, admin, f.provider.Issuer, "Unmapped-Group", "", authorization.RoleNone, authorization.RoleAdmin); err != nil {
		t.Fatal(err)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone {
		t.Fatalf("group names matched case-insensitively: %q", role)
	}

	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.group_mapping.change", Outcome: AuditSucceeded, Limit: 10})
	if err != nil || len(events) != 3 {
		t.Fatalf("mapping audit = %+v, %v", events, err)
	}
	if e := events[1]; e.ResourceKind != "cognito_group" || e.ResourceID != f.provider.Issuer+"#unmapped-group" ||
		e.ClusterID != "cluster-a" || e.BeforeState != "OPERATOR" || e.AfterState != "" || e.PolicyRevision != 2 || e.ActorID != "admin" {
		t.Fatalf("removal audit = %+v", e)
	}
}

func TestGroupMappingChangesRequireGlobalAdminAndCurrentPolicy(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	admin := groupMappingAdmin(t, f, "admin", authorization.RoleAdmin)
	operator := groupMappingAdmin(t, f, "operator", authorization.RoleOperator)
	if _, err := f.db.db.ExecContext(ctx, "INSERT INTO role_grants (account_id, cluster_id, role) VALUES ('operator', 'cluster-a', 'ADMIN')"); err != nil {
		t.Fatal(err)
	}
	mappings := groupMappingService(t, f)
	if _, err := mappings.Execute(ctx, operator, f.provider.Issuer, "unmapped-group", "cluster-a", authorization.RoleNone, authorization.RoleAdmin); !errors.Is(err, identity.ErrGlobalAdminRequired) {
		t.Fatalf("cluster ADMIN mapping error = %v", err)
	}
	if _, err := mappings.Execute(ctx, admin, f.provider.Issuer, "unmapped-group", "", authorization.RoleViewer, authorization.RoleAdmin); !errors.Is(err, identity.ErrPolicyConflict) {
		t.Fatalf("stale mapping error = %v", err)
	}
	if _, err := mappings.Execute(ctx, admin, "http://issuer.example", "unmapped-group", "", authorization.RoleNone, authorization.RoleAdmin); !errors.Is(err, identity.ErrInvalidAccessChange) {
		t.Fatalf("invalid issuer error = %v", err)
	}
	var count int
	if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_group_bindings").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected changes wrote mappings: %d %v", count, err)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone {
		t.Fatalf("rejected changes granted access: %q", role)
	}
	denied, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.group_mapping.change", Outcome: AuditDenied, Limit: 10})
	if err != nil || len(denied) != 1 || denied[0].ActorID != "operator" {
		t.Fatalf("cluster ADMIN denial audit = %+v, %v", denied, err)
	}
	if _, _, err := f.sessions.ResolveGrants(ctx, f.credentials.Token, "cluster-a"); errors.Is(err, session.ErrInvalidSession) {
		t.Fatal("mapping changes invalidated the Cognito session")
	}
}
