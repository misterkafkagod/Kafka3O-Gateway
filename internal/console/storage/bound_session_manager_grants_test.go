package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionManagerGrantsResolveLocalAndEnforceRestrictions(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	record, token := browserSessionRecord("manager-grants", "browser-local-a", binding, f.clock())
	unbound, unboundToken := browserSessionRecord("manager-grants-unbound", "browser-local-a", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CreateLocalSession(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec(`INSERT INTO role_grants (account_id, cluster_id, role)
VALUES (?, '', 'VIEWER'), (?, 'cluster-a', 'OPERATOR')`, record.AccountID, record.AccountID); err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		cluster string
		role    authorization.Role
	}{
		{"", authorization.RoleViewer},
		{"cluster-a", authorization.RoleOperator},
		{"cluster-b", authorization.RoleViewer},
	} {
		principal, grants, err := manager.ResolveBoundGrants(ctx, token, binding, tt.cluster)
		if err != nil || principal.SessionID != record.SessionID || principal.Source != session.SourceLocal {
			t.Fatalf("bound local manager could not resolve grants: %v", err)
		}
		if role, err := authorization.ResolveRole(grants, tt.cluster); err != nil || role != tt.role {
			t.Fatalf("bound local grant scope %q did not resolve correctly: %v", tt.cluster, err)
		}
		for _, secret := range []string{marker, token, base64.RawURLEncoding.EncodeToString(binding[:])} {
			if strings.Contains(fmt.Sprint(principal, grants), secret) {
				t.Fatal("bound manager exposed a browser or session credential")
			}
		}
	}
	wrong := sha256.Sum256([]byte("other grant browser"))
	for _, bad := range [][32]byte{{}, wrong} {
		if principal, grants, err := manager.ResolveBoundGrants(ctx, token, bad, "cluster-a"); err != session.ErrInvalidSession ||
			principal.SessionID != "" || len(grants) != 0 {
			t.Fatalf("bound manager granted a missing or wrong browser: %v", err)
		}
	}
	if principal, grants, err := manager.ResolveGrants(ctx, token, "cluster-a"); err != session.ErrInvalidSession ||
		principal.SessionID != "" || len(grants) != 0 {
		t.Fatalf("unbound grant manager accepted a bound session: %v", err)
	}
	if principal, grants, err := manager.ResolveBoundGrants(ctx, unboundToken, binding, "cluster-a"); err != session.ErrInvalidSession ||
		principal.SessionID != "" || len(grants) != 0 {
		t.Fatalf("bound grant manager accepted an unbound session: %v", err)
	}
	if principal, grants, err := manager.ResolveGrants(ctx, unboundToken, "cluster-a"); err != nil ||
		principal.SessionID != unbound.SessionID || len(grants) != 2 {
		t.Fatalf("bound grant integration changed unbound behavior: %v", err)
	}
	if _, err := f.db.db.Exec("UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", record.AccountID); err != nil {
		t.Fatal(err)
	}
	if principal, grants, err := manager.ResolveBoundGrants(ctx, token, binding, "cluster-a"); err != session.ErrPasswordChangeRequired ||
		principal.SessionID != "" || len(grants) != 0 {
		t.Fatalf("bound grants bypassed mandatory password change: %v", err)
	}
	if _, err := f.db.db.Exec("UPDATE local_accounts SET must_change_password = 0 WHERE identity_id = ?", record.AccountID); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	manager, err = session.NewManager(f.db, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	if principal, grants, err := manager.ResolveBoundGrants(ctx, token, binding, "cluster-a"); err != nil ||
		principal.SessionID != record.SessionID || len(grants) != 2 {
		t.Fatalf("bound grant resolution did not survive restart: %v", err)
	}
}

func TestBoundSessionManagerGrantsFollowVerifiedRefresh(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	if _, err := f.db.db.Exec(`INSERT INTO cognito_group_bindings (issuer, group_name, cluster_id, role)
VALUES (?, 'unmapped-group', '', 'VIEWER'), (?, 'refreshed-group', 'cluster-a', 'ADMIN')`,
		f.provider.Issuer, f.provider.Issuer); err != nil {
		t.Fatal(err)
	}
	principal, grants, err := f.sessions.ResolveBoundGrants(ctx, f.credentials.Token, f.binding, "cluster-a")
	if err != nil || principal.SessionID != f.credentials.Principal.SessionID {
		t.Fatalf("fresh bound Cognito grants failed: %v", err)
	}
	if role, err := authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleViewer {
		t.Fatal("initial verified group did not resolve")
	}
	f.current.Store(f.freshUntil)
	principal, grants, err = f.sessions.ResolveBoundGrants(ctx, f.credentials.Token, f.binding, "cluster-a")
	if err != nil || principal.SessionID != f.credentials.Principal.SessionID || f.refreshCalls.Load() != 1 {
		t.Fatalf("bound Cognito grant resolution did not refresh: %v", err)
	}
	if role, err := authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleAdmin {
		t.Fatal("bound grants did not use the refreshed verified group")
	}
	if _, grants, err := f.sessions.ResolveBoundGrants(ctx, f.credentials.Token, f.binding, "cluster-b"); err != nil || len(grants) != 0 {
		t.Fatalf("previous or out-of-scope group grant survived refresh: %v", err)
	}
	for _, secret := range []string{f.marker, base64.RawURLEncoding.EncodeToString(f.binding[:]), f.credentials.Token,
		f.provider.Issuer, "unmapped-group", "refreshed-group", "opaque|synthetic-subject", fixtureRefresh, fixtureRotated} {
		if strings.Contains(fmt.Sprint(principal, grants), secret) {
			t.Fatal("bound grant manager exposed verified claims or credentials")
		}
	}
	f.reopenManager(t)
	if _, grants, err := f.sessions.ResolveBoundGrants(ctx, f.credentials.Token, f.binding, "cluster-a"); err != nil || f.refreshCalls.Load() != 1 {
		t.Fatalf("refreshed bound grants did not survive restart: %v", err)
	} else if role, err := authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleAdmin {
		t.Fatal("persisted verified group did not resolve after restart")
	}
}

func TestBoundSessionManagerGrantsFailClosedOnRefreshOutage(t *testing.T) {
	f := newBoundRefreshingSessionFixture(t, "refresh outage")
	f.current.Store(f.freshUntil)
	for i := 0; i < 2; i++ {
		principal, grants, err := f.sessions.ResolveBoundGrants(context.Background(), f.credentials.Token, f.binding, "cluster-a")
		if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || errors.Is(err, session.ErrInvalidSession) ||
			principal.SessionID != "" || len(grants) != 0 || f.refreshCalls.Load() != 1 {
			t.Fatalf("bound grants exposed or replayed unavailable refresh: %v", err)
		}
	}
}
