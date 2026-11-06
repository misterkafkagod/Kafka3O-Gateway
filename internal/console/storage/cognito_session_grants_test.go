package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func grantFixtureHash(t *testing.T, token string) [32]byte {
	t.Helper()
	value, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(value)
}

func addFixtureGroupBinding(t *testing.T, f *refreshingSessionFixture, issuer, group, cluster string, role authorization.Role) {
	t.Helper()
	if _, err := f.db.db.Exec(`INSERT INTO cognito_group_bindings (issuer, group_name, cluster_id, role)
VALUES (?, ?, ?, ?)`, issuer, group, cluster, role); err != nil {
		t.Fatal(err)
	}
}

func fixtureResolvedRole(t *testing.T, f *refreshingSessionFixture, cluster string) (authorization.Role, []authorization.Grant) {
	t.Helper()
	principal, grants, err := f.sessions.ResolveGrants(context.Background(), f.credentials.Token, cluster)
	if err != nil || principal.AccountID != f.credentials.Principal.AccountID {
		t.Fatalf("resolve session grants: %v", err)
	}
	role, err := authorization.ResolveRole(grants, cluster)
	if err != nil {
		t.Fatal(err)
	}
	return role, grants
}

func TestCognitoSessionGrantsRequireExactConfiguredBindings(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	if role, grants := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone || len(grants) != 0 {
		t.Fatal("an unmapped Cognito identity received a role")
	}
	for _, tt := range []struct {
		issuer, group string
	}{
		{f.provider.Issuer, "UNMAPPED-GROUP"},
		{f.provider.Issuer, "unmapped"},
		{f.provider.Issuer, "unmapped-group-extra"},
		{f.provider.Issuer + "/other", "unmapped-group"},
	} {
		addFixtureGroupBinding(t, f, tt.issuer, tt.group, "", authorization.RoleAdmin)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone {
		t.Fatal("case, prefix or issuer mismatch granted a role")
	}
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleViewer)
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "cluster-a", authorization.RoleAdmin)
	if _, err := f.db.db.Exec("INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, 'cluster-b', 'OPERATOR')",
		f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		cluster string
		want    authorization.Role
	}{
		{"", authorization.RoleViewer},
		{"cluster-a", authorization.RoleAdmin},
		{"cluster-b", authorization.RoleOperator},
		{"cluster-c", authorization.RoleViewer},
	} {
		role, grants := fixtureResolvedRole(t, f, tt.cluster)
		if role != tt.want {
			t.Fatalf("scope %q: role=%q want=%q", tt.cluster, role, tt.want)
		}
		serialized := fmt.Sprint(grants)
		for _, secret := range []string{f.provider.Issuer, "unmapped-group", "opaque|synthetic-subject",
			f.credentials.Token, f.credentials.CSRFToken, fixtureRefresh} {
			if strings.Contains(serialized, secret) {
				t.Fatal("session grants exposed verified claims or credentials")
			}
		}
	}
	_, scoped := fixtureResolvedRole(t, f, "cluster-a")
	for _, tt := range []struct {
		cluster     string
		environment authorization.Environment
		reason      string
		allowed     bool
	}{
		{"cluster-a", authorization.EnvironmentProduction, "", false},
		{"cluster-a", authorization.EnvironmentProduction, "approved change", true},
		{"cluster-b", authorization.EnvironmentProduction, "approved change", false},
		{"cluster-b", authorization.EnvironmentNonProduction, "", true},
	} {
		_, grants := fixtureResolvedRole(t, f, tt.cluster)
		decision, err := authorization.EvaluatePolicy(authorization.PolicyRequest{
			Grants: grants, ClusterID: tt.cluster, Environment: tt.environment,
			Action: authorization.ActionProduceMessage, ChangeReason: tt.reason,
		})
		if err != nil || decision.Allowed != tt.allowed {
			t.Fatalf("scope %q production policy: decision=%+v err=%v", tt.cluster, decision, err)
		}
	}
	if leaked, err := authorization.ResolveRole(scoped, "cluster-b"); err != nil || leaked != authorization.RoleViewer {
		t.Fatalf("cluster-a grant escaped its scope: role=%q err=%v", leaked, err)
	}
	if _, err := f.db.db.Exec(`DELETE FROM cognito_group_bindings WHERE issuer = ? AND group_name = ? AND cluster_id = 'cluster-a'`,
		f.provider.Issuer, "unmapped-group"); err != nil {
		t.Fatal(err)
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleViewer {
		t.Fatal("a removed group binding remained effective on the next resolution")
	}
}

func TestCognitoSessionGrantsFollowVerifiedRefresh(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleViewer)
	addFixtureGroupBinding(t, f, f.provider.Issuer, "refreshed-group", "cluster-a", authorization.RoleAdmin)
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleViewer {
		t.Fatal("initial verified group was not mapped")
	}
	f.makeStale()
	if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleAdmin || f.refreshCalls.Load() != 1 {
		t.Fatal("refreshed verified group did not replace the previous snapshot")
	}
	if role, _ := fixtureResolvedRole(t, f, "cluster-b"); role != authorization.RoleNone || f.refreshCalls.Load() != 1 {
		t.Fatal("removed group or another cluster's binding remained effective")
	}
}

func TestCognitoSessionGrantsCombineDirectAndGroupRoles(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleViewer)
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "cluster-a", authorization.RoleAdmin)
	if _, err := f.db.db.Exec(`INSERT INTO role_grants (account_id, cluster_id, role)
VALUES (?, '', 'OPERATOR'), (?, 'cluster-a', 'VIEWER')`,
		f.credentials.Principal.AccountID, f.credentials.Principal.AccountID); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		cluster string
		want    authorization.Role
	}{
		{"", authorization.RoleOperator},
		{"cluster-a", authorization.RoleAdmin},
		{"cluster-b", authorization.RoleOperator},
	} {
		role, grants := fixtureResolvedRole(t, f, tt.cluster)
		if role != tt.want || len(grants) > 2 {
			t.Fatalf("scope %q: role=%q grants=%v", tt.cluster, role, grants)
		}
	}
}

func TestCognitoSessionGrantsDoNotApplyGroupsToLocalSessions(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleAdmin)
	if _, err := f.db.db.Exec(`INSERT INTO identities (id, enabled) VALUES ('local-user', 1);
INSERT INTO local_accounts (identity_id, username, password_hash, must_change_password, credential_version)
VALUES ('local-user', 'local-user', 'synthetic-hash', 0, 1);
INSERT INTO role_grants (account_id, cluster_id, role) VALUES ('local-user', '', 'VIEWER')`); err != nil {
		t.Fatal(err)
	}
	local, err := f.sessions.IssueLocal(context.Background(), identity.LocalAccount{ID: "local-user", Enabled: true, CredentialVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	principal, grants, err := f.sessions.ResolveGrants(context.Background(), local.Token, "cluster-a")
	if err != nil || principal.Source != session.SourceLocal {
		t.Fatalf("local grant resolution failed: %v", err)
	}
	role, err := authorization.ResolveRole(grants, "cluster-a")
	if err != nil || role != authorization.RoleViewer {
		t.Fatalf("Cognito group binding affected a local session: role=%q err=%v", role, err)
	}
	if _, err := f.db.db.Exec("UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = 'local-user'"); err != nil {
		t.Fatal(err)
	}
	if principal, grants, err := f.sessions.ResolveGrants(context.Background(), local.Token, "cluster-a"); !errors.Is(err, session.ErrPasswordChangeRequired) || principal.SessionID != "" || len(grants) != 0 {
		t.Fatalf("temporary-password session received grants: %v", err)
	}
	if _, err := f.db.db.Exec(`UPDATE local_accounts SET must_change_password = 0 WHERE identity_id = 'local-user';
DROP TABLE cognito_group_bindings`); err != nil {
		t.Fatal(err)
	}
	_, grants, err = f.sessions.ResolveGrants(context.Background(), local.Token, "cluster-a")
	if err != nil {
		t.Fatalf("local grants depended on Cognito mapping availability: %v", err)
	}
	if role, err = authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleViewer {
		t.Fatalf("local direct grant changed with Cognito mapping unavailable: role=%q err=%v", role, err)
	}
}

func TestCognitoSessionGrantStateFailuresReturnNoGrants(t *testing.T) {
	for _, mode := range []string{"pending", "disabled", "revoked", "expired", "key", "issuer", "subject", "groups", "utf8", "missing credentials", "policy store"} {
		t.Run(mode, func(t *testing.T) {
			f := newRefreshingSessionFixture(t, "refresh rotated")
			ctx := context.Background()
			addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleAdmin)
			switch mode {
			case "pending":
				f.makeStale()
				if _, err := f.db.ReserveCognitoRefresh(ctx, cognitoRefreshRequest(t, f.credentials, f.protector, f.clock())); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := f.sessions.Revoke(ctx, f.credentials.Token); err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
			case "key":
				wrong, err := cognito.NewRefreshTokenProtector(f.manager, rand.Reader, bytes.Repeat([]byte{8}, 32))
				if err != nil {
					t.Fatal(err)
				}
				f.sessions, err = session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock,
					session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, wrong)
				if err != nil {
					t.Fatal(err)
				}
			case "issuer":
				if _, err := f.db.db.Exec("UPDATE cognito_identities SET issuer = ? WHERE identity_id = ?",
					f.provider.Issuer+"/other", f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "subject":
				if _, err := f.db.db.Exec("UPDATE cognito_identities SET subject = '' WHERE identity_id = ?",
					f.credentials.Principal.AccountID); err != nil {
					t.Fatal(err)
				}
			case "groups":
				if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET groups_json = ? WHERE session_id = ?",
					`["unmapped-group",null]`, f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			case "utf8":
				if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET groups_json = CAST(x'5b22ff225d' AS TEXT) WHERE session_id = ?",
					f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			case "missing credentials":
				if _, err := f.db.db.Exec("DELETE FROM cognito_session_credentials WHERE session_id = ?", f.credentials.Principal.SessionID); err != nil {
					t.Fatal(err)
				}
			case "policy store":
				if _, err := f.db.db.Exec("DROP TABLE cognito_group_bindings"); err != nil {
					t.Fatal(err)
				}
			}
			principal, grants, err := f.sessions.ResolveGrants(ctx, f.credentials.Token, "cluster-a")
			if err == nil || principal.SessionID != "" || len(grants) != 0 {
				t.Fatalf("%s session returned grants or a principal: %v", mode, err)
			}
			switch mode {
			case "disabled", "revoked", "expired":
				if err != session.ErrInvalidSession {
					t.Fatalf("%s session was not definitely invalid: %v", mode, err)
				}
			case "pending", "issuer", "subject", "groups", "utf8", "missing credentials":
				if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || err == session.ErrInvalidSession {
					t.Fatalf("%s state was mistaken for a definite invalid session: %v", mode, err)
				}
			}
			if f.refreshCalls.Load() != 0 {
				t.Fatal("invalid or unresolved state dispatched a provider request")
			}
		})
	}
}

func TestCognitoSessionGrantsRecheckExactDeadlines(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleViewer)
	hash := grantFixtureHash(t, f.credentials.Token)
	ctx := context.Background()
	f.current.Store(f.freshUntil - 1)
	grants, err := f.db.ResolveSessionGrants(ctx, hash, "cluster-a", f.clock(), f.protector)
	if err != nil || len(grants) != 1 || grants[0].Role != authorization.RoleViewer {
		t.Fatalf("fresh grants were denied before the deadline: %v", err)
	}
	f.current.Store(f.freshUntil)
	grants, err = f.db.ResolveSessionGrants(ctx, hash, "cluster-a", f.clock(), f.protector)
	if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || len(grants) != 0 {
		t.Fatalf("stale grants remained available at the deadline: %v", err)
	}
	f.current.Store(f.credentials.Principal.IdleExpiresAt.UnixNano())
	grants, err = f.db.ResolveSessionGrants(ctx, hash, "cluster-a", f.clock(), f.protector)
	if err != session.ErrInvalidSession || len(grants) != 0 {
		t.Fatalf("expired session retained grants: %v", err)
	}
}

func TestCognitoSessionGrantDeadlinesAtEachBoundary(t *testing.T) {
	boundary := time.Unix(1_800_000_000, 0)
	for _, tt := range []struct {
		name string
		set  func(*sessionGrantDeadlines)
		want error
	}{
		{"idle", func(d *sessionGrantDeadlines) { d.idle = boundary.UnixNano() }, session.ErrInvalidSession},
		{"absolute", func(d *sessionGrantDeadlines) { d.absolute = boundary.UnixNano() }, session.ErrInvalidSession},
		{"freshness", func(d *sessionGrantDeadlines) { d.freshness = boundary.UnixNano() }, session.ErrCognitoRefreshUnavailable},
		{"token expiry", func(d *sessionGrantDeadlines) { d.tokenExpiry = boundary.UnixNano() }, session.ErrCognitoRefreshUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			later := boundary.Add(time.Hour).UnixNano()
			deadlines := sessionGrantDeadlines{idle: later, absolute: later, freshness: later, tokenExpiry: later, cognito: true}
			tt.set(&deadlines)
			if err := validateSessionGrantDeadlines(deadlines, boundary.Add(-time.Nanosecond)); err != nil {
				t.Fatalf("deadline rejected one nanosecond early: %v", err)
			}
			for _, now := range []time.Time{boundary, boundary.Add(time.Nanosecond)} {
				if err := validateSessionGrantDeadlines(deadlines, now); err != tt.want {
					t.Fatalf("deadline at %s: got %v, want %v", now, err, tt.want)
				}
			}
		})
	}
}

func TestCognitoSessionGrantsRecheckQueuedAndCommittedFreshness(t *testing.T) {
	for _, stage := range []int{0, 2, 3} {
		t.Run(fmt.Sprintf("stage=%d", stage), func(t *testing.T) {
			f := newRefreshingSessionFixture(t, "refresh rotated")
			addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleAdmin)
			f.current.Store(f.freshUntil - 1)
			requested := f.clock()
			hash := grantFixtureHash(t, f.credentials.Token)
			var grants []authorization.Grant
			var err error
			if stage == 0 {
				err = runQueuedDatabaseOperation(t, f.db, func() {
					f.current.Store(f.freshUntil)
				}, func() error {
					grants, err = f.db.ResolveSessionGrants(context.Background(), hash, "cluster-a", requested, f.protector)
					return err
				})
			} else {
				calls := 0
				f.db.now = func() time.Time {
					calls++
					if calls >= stage {
						f.current.Store(f.freshUntil)
					}
					return f.clock()
				}
				grants, err = f.db.ResolveSessionGrants(context.Background(), hash, "cluster-a", requested, f.protector)
			}
			if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || len(grants) != 0 {
				t.Fatalf("queued or expired grant snapshot escaped: %v", err)
			}
		})
	}
}

func TestCognitoSessionGrantsFailClosedOnRefreshOutageAndInvalidPolicy(t *testing.T) {
	t.Run("refresh outage", func(t *testing.T) {
		f := newRefreshingSessionFixture(t, "refresh outage")
		addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", authorization.RoleAdmin)
		f.makeStale()
		for i := 0; i < 2; i++ {
			principal, grants, err := f.sessions.ResolveGrants(context.Background(), f.credentials.Token, "cluster-a")
			if !errors.Is(err, session.ErrCognitoRefreshUnavailable) || err == session.ErrInvalidSession ||
				principal.SessionID != "" || len(grants) != 0 || f.refreshCalls.Load() != 1 {
				t.Fatalf("outage granted access or retried a refresh: %v", err)
			}
		}
	})
	t.Run("invalid policy", func(t *testing.T) {
		f := newRefreshingSessionFixture(t, "refresh rotated")
		if _, err := f.db.db.Exec("PRAGMA ignore_check_constraints = ON"); err != nil {
			t.Fatal(err)
		}
		addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "", "OWNER")
		principal, grants, err := f.sessions.ResolveGrants(context.Background(), f.credentials.Token, "cluster-a")
		if !errors.Is(err, authorization.ErrInvalidRole) || principal.SessionID != "" || len(grants) != 0 {
			t.Fatalf("invalid configured role granted access: %v", err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		f := newRefreshingSessionFixture(t, "refresh rotated")
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		grants, err := f.db.ResolveSessionGrants(cancelled, grantFixtureHash(t, f.credentials.Token), "cluster-a", f.clock(), f.protector)
		if !errors.Is(err, context.Canceled) || len(grants) != 0 {
			t.Fatalf("canceled grant resolution returned grants: %v", err)
		}
	})
}

func TestCognitoGroupBindingVersionEightMigrationAndRestart(t *testing.T) {
	f := newRefreshingSessionFixture(t, "refresh rotated")
	ctx := context.Background()
	if _, err := f.db.db.Exec("DROP TABLE cognito_group_bindings; PRAGMA user_version = 8"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		f.reopen(t)
		var err error
		f.exchanger, err = cognito.NewCodeExchanger(f.manager, f.verifier, f.callback, cognito.ClientAuthenticationPublic, "")
		if err != nil {
			t.Fatal(err)
		}
		_, f.protector = cognitoSessionTools(t, f.cognitoCodeFixture)
		f.sessions, err = session.NewRefreshingCognitoManager(f.db, rand.Reader, f.clock,
			session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime, f.exchanger, f.protector)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			var version, bindings int
			if err := f.db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 9 {
				t.Fatalf("version-eight migration did not advance: version=%d err=%v", version, err)
			}
			if err := f.db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cognito_group_bindings").Scan(&bindings); err != nil || bindings != 0 {
				t.Fatalf("migration created implicit group grants: count=%d err=%v", bindings, err)
			}
			if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleNone {
				t.Fatal("migration changed existing session access")
			}
			addFixtureGroupBinding(t, f, f.provider.Issuer, "unmapped-group", "cluster-a", authorization.RoleOperator)
		} else if role, _ := fixtureResolvedRole(t, f, "cluster-a"); role != authorization.RoleOperator {
			t.Fatal("configured group binding or session was lost on restart")
		}
	}
}
