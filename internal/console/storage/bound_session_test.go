package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func TestBoundSessionLocalResolutionAndGrants(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	bound, token := browserSessionRecord("bound-local", "browser-local-a", binding, f.clock())
	unbound, _ := browserSessionRecord("unbound-local", "browser-local-a", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, bound); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CreateLocalSession(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec(`INSERT INTO role_grants (account_id, cluster_id, role)
VALUES (?, '', 'VIEWER'), (?, 'cluster-a', 'OPERATOR')`, bound.AccountID, bound.AccountID); err != nil {
		t.Fatal(err)
	}
	principal, err := f.db.UseBoundSession(ctx, bound.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil)
	if err != nil || principal.SessionID != bound.SessionID || principal.AccountID != bound.AccountID ||
		principal.Source != session.SourceLocal || principal.CSRFHash != bound.CSRFHash {
		t.Fatalf("bound local session did not resolve: %v", err)
	}
	grants, err := f.db.ResolveBoundSessionGrants(ctx, bound.TokenHash, binding, "cluster-a", f.clock(), nil)
	if err != nil || len(grants) != 2 {
		t.Fatalf("bound local grants did not resolve: %v", err)
	}
	if role, err := authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleOperator {
		t.Fatal("bound local grants did not respect their scope")
	}
	if strings.Contains(fmt.Sprint(principal, grants), marker) || strings.Contains(fmt.Sprint(principal, grants), token) {
		t.Fatal("bound resolution exposed browser or session credentials")
	}
	var idleBefore int64
	if err := f.db.db.QueryRow("SELECT idle_expires_at_ns FROM console_sessions WHERE id = ?", bound.SessionID).Scan(&idleBefore); err != nil {
		t.Fatal(err)
	}
	f.current.Store(f.clock().Add(time.Minute).UnixNano())
	wrong := sha256.Sum256([]byte("another browser"))
	for _, tt := range []struct {
		name        string
		tokenHash   [32]byte
		browserHash [32]byte
	}{
		{"missing marker", bound.TokenHash, [32]byte{}},
		{"wrong marker", bound.TokenHash, wrong},
		{"unbound token", unbound.TokenHash, binding},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if principal, err := f.db.UseBoundSession(ctx, tt.tokenHash, tt.browserHash, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession || principal.SessionID != "" {
				t.Fatal("wrong session or marker was accepted")
			}
			if grants, err := f.db.ResolveBoundSessionGrants(ctx, tt.tokenHash, tt.browserHash, "cluster-a", f.clock(), nil); err != session.ErrInvalidSession || len(grants) != 0 {
				t.Fatal("wrong session or marker received grants")
			}
		})
	}
	var idleAfter int64
	if err := f.db.db.QueryRow("SELECT idle_expires_at_ns FROM console_sessions WHERE id = ?", bound.SessionID).Scan(&idleAfter); err != nil || idleAfter != idleBefore {
		t.Fatal("wrong marker extended the bound session")
	}
	if _, err := f.db.UseSession(ctx, bound.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession {
		t.Fatal("unbound resolver accepted a bound session")
	}
	f.reopen(t)
	if _, err := f.db.UseBoundSession(ctx, bound.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != nil {
		t.Fatalf("bound session did not survive restart: %v", err)
	}
	if _, err := f.db.db.Exec("DELETE FROM role_grants WHERE account_id = ? AND cluster_id = 'cluster-a'", bound.AccountID); err != nil {
		t.Fatal(err)
	}
	grants, err = f.db.ResolveBoundSessionGrants(ctx, bound.TokenHash, binding, "cluster-a", f.clock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if role, err := authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleViewer {
		t.Fatal("bound grant resolution retained a removed grant")
	}
}

func TestBoundSessionCognitoFreshnessAndGroupGrants(t *testing.T) {
	f, binding, marker := newBrowserSessionFixture(t)
	ctx := context.Background()
	record, token, protector := browserCognitoRecord(t, f, "bound-cognito", binding)
	principal, err := f.db.CreateCognitoSession(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec(`INSERT INTO cognito_group_bindings (issuer, group_name, cluster_id, role)
VALUES (?, 'unmapped-group', '', 'VIEWER'), (?, 'unmapped-group', 'cluster-a', 'ADMIN')`,
		f.provider.Issuer, f.provider.Issuer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, 'cluster-b', 'OPERATOR')", principal.AccountID); err != nil {
		t.Fatal(err)
	}
	resolved, err := f.db.UseBoundSession(ctx, record.Session.TokenHash, binding, f.clock, session.DefaultIdleLifetime, protector)
	if err != nil || resolved.AccountID != principal.AccountID || resolved.Source != session.SourceCognito {
		t.Fatalf("fresh bound Cognito session did not resolve: %v", err)
	}
	for _, tt := range []struct {
		cluster string
		role    authorization.Role
	}{
		{"cluster-a", authorization.RoleAdmin},
		{"cluster-b", authorization.RoleOperator},
		{"cluster-c", authorization.RoleViewer},
	} {
		grants, err := f.db.ResolveBoundSessionGrants(ctx, record.Session.TokenHash, binding, tt.cluster, f.clock(), protector)
		if err != nil {
			t.Fatal(err)
		}
		if role, err := authorization.ResolveRole(grants, tt.cluster); err != nil || role != tt.role {
			t.Fatalf("bound Cognito role for %q was incorrect", tt.cluster)
		}
		for _, secret := range []string{marker, token, f.provider.Issuer, "unmapped-group", "opaque|synthetic-subject", fixtureRefresh} {
			if strings.Contains(fmt.Sprint(grants, resolved), secret) {
				t.Fatal("bound Cognito resolution exposed credentials or verified claims")
			}
		}
	}
	if _, err := f.db.db.Exec("DELETE FROM cognito_group_bindings WHERE issuer = ? AND cluster_id = 'cluster-a'", f.provider.Issuer); err != nil {
		t.Fatal(err)
	}
	grants, err := f.db.ResolveBoundSessionGrants(ctx, record.Session.TokenHash, binding, "cluster-a", f.clock(), protector)
	if err != nil {
		t.Fatal(err)
	}
	if role, err := authorization.ResolveRole(grants, "cluster-a"); err != nil || role != authorization.RoleViewer {
		t.Fatal("removed Cognito group binding remained effective")
	}
	if _, err := f.db.UseBoundSession(ctx, record.Session.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession {
		t.Fatal("bound Cognito session resolved without its protector")
	}
	if grants, err := f.db.ResolveBoundSessionGrants(ctx, record.Session.TokenHash, binding, "", f.clock(), nil); !errors.Is(err, session.ErrCognitoRefreshUnavailable) || len(grants) != 0 {
		t.Fatal("bound Cognito grants resolved without their protector")
	}
	f.current.Store(f.clock().Add(5 * time.Minute).UnixNano())
	if _, err := f.db.UseBoundSession(ctx, record.Session.TokenHash, binding, f.clock, session.DefaultIdleLifetime, protector); err != session.ErrInvalidSession {
		t.Fatal("stale Cognito claims resolved a bound session")
	}
	if grants, err := f.db.ResolveBoundSessionGrants(ctx, record.Session.TokenHash, binding, "cluster-a", f.clock(), protector); !errors.Is(err, session.ErrCognitoRefreshUnavailable) || len(grants) != 0 {
		t.Fatal("stale Cognito claims granted browser-bound access")
	}
	if f.refreshCalls.Load() != 0 {
		t.Fatal("bound resolution attempted an unbound provider refresh")
	}
}

func TestBoundSessionRejectsInactiveAndCorruptBindings(t *testing.T) {
	for _, mode := range []string{"revoked", "disabled", "idle", "absolute", "credential-version", "missing-mapping", "changed-mapping", "changed-session-digest", "missing-table"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			record, _ := browserSessionRecord("invalid-"+mode, "browser-local-a", binding, f.clock())
			other, _ := browserSessionRecord("other-"+mode, "browser-local-b", [32]byte{}, f.clock())
			if _, err := f.db.CreateLocalSession(ctx, record); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.CreateLocalSession(ctx, other); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "revoked":
				if err := f.db.RevokeBoundSession(ctx, record.TokenHash, binding, "bound-revocation", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "idle":
				f.current.Store(record.IdleExpiresAt.UnixNano())
			case "absolute":
				if _, err := f.db.db.Exec("UPDATE console_sessions SET idle_expires_at_ns = absolute_expires_at_ns WHERE id = ?", record.SessionID); err != nil {
					t.Fatal(err)
				}
				f.current.Store(record.AbsoluteExpiresAt.UnixNano())
			case "credential-version":
				if _, err := f.db.db.Exec("UPDATE local_accounts SET credential_version = 2 WHERE identity_id = ?", record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "missing-mapping":
				if _, err := f.db.db.Exec("DELETE FROM console_browser_sessions WHERE browser_hash = ?", binding[:]); err != nil {
					t.Fatal(err)
				}
			case "changed-mapping":
				if _, err := f.db.db.Exec("UPDATE console_browser_sessions SET session_id = ? WHERE browser_hash = ?", other.SessionID, binding[:]); err != nil {
					t.Fatal(err)
				}
			case "changed-session-digest":
				if _, err := f.db.db.Exec("UPDATE console_sessions SET browser_binding_hash = zeroblob(32) WHERE id = ?", record.SessionID); err != nil {
					t.Fatal(err)
				}
			case "missing-table":
				if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
					t.Fatal(err)
				}
			}
			principal, useErr := f.db.UseBoundSession(ctx, record.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil)
			grants, grantErr := f.db.ResolveBoundSessionGrants(ctx, record.TokenHash, binding, "", f.clock(), nil)
			if principal.SessionID != "" || len(grants) != 0 || useErr == nil || grantErr == nil {
				t.Fatal("inactive or corrupt browser binding returned access")
			}
			if mode == "missing-table" {
				if useErr == session.ErrInvalidSession || grantErr == session.ErrInvalidSession {
					t.Fatal("missing binding storage was hidden as an invalid session")
				}
				if _, err := f.db.UseSession(ctx, other.TokenHash, f.clock, session.DefaultIdleLifetime, nil); err != nil {
					t.Fatalf("unbound session depended on the missing binding table: %v", err)
				}
				if _, err := f.db.ResolveSessionGrants(ctx, other.TokenHash, "", f.clock(), nil); err != nil {
					t.Fatalf("unbound grants depended on the missing binding table: %v", err)
				}
			} else if useErr != session.ErrInvalidSession || grantErr != session.ErrInvalidSession {
				t.Fatalf("inactive binding was not rejected as invalid: use=%v grants=%v", useErr, grantErr)
			}
		})
	}
}

func TestBoundSessionDoesNotReviveAfterReplacement(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	first, _ := browserSessionRecord("replaced-old", "browser-local-a", binding, f.clock())
	second, _ := browserSessionRecord("replaced-new", "browser-local-b", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", first.AccountID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CreateLocalSession(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", first.AccountID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.UseBoundSession(ctx, first.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSession {
		t.Fatal("old session revived after its browser binding was replaced")
	}
	if grants, err := f.db.ResolveBoundSessionGrants(ctx, first.TokenHash, binding, "", f.clock(), nil); err != session.ErrInvalidSession || len(grants) != 0 {
		t.Fatal("old session regained grants after browser binding replacement")
	}
	if principal, err := f.db.UseBoundSession(ctx, second.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != nil || principal.SessionID != second.SessionID {
		t.Fatalf("replacement session was not current: %v", err)
	}
}

func TestBoundSessionQueuedBehindBindingReplacement(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	first, _ := browserSessionRecord("queued-old", "browser-local-a", binding, f.clock())
	other, _ := browserSessionRecord("queued-other", "browser-local-b", [32]byte{}, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CreateLocalSession(ctx, other); err != nil {
		t.Fatal(err)
	}
	tx, err := f.db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE console_browser_sessions SET session_id = ? WHERE browser_hash = ?", other.SessionID, binding[:]); err != nil {
		t.Fatal(err)
	}
	waitCount := f.db.db.Stats().WaitCount
	results := make(chan error, 2)
	go func() {
		_, err := f.db.UseBoundSession(ctx, first.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil)
		results <- err
	}()
	go func() {
		_, err := f.db.ResolveBoundSessionGrants(ctx, first.TokenHash, binding, "", f.clock(), nil)
		results <- err
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for f.db.db.Stats().WaitCount < waitCount+2 {
		select {
		case <-timer.C:
			t.Fatal("bound checks did not wait for the pending replacement")
		case <-ticker.C:
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case err := <-results:
			if err != session.ErrInvalidSession {
				t.Fatalf("queued bound check accepted a replaced binding: %v", err)
			}
		case <-timer.C:
			t.Fatal("queued bound checks did not finish")
		}
	}
}

func TestBoundSessionPasswordChangeRestriction(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	record, _ := browserSessionRecord("password-change", "browser-local-a", binding, f.clock())
	if _, err := f.db.CreateLocalSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", record.AccountID); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.db.UseBoundSession(ctx, record.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != nil || !principal.MustChangePassword {
		t.Fatal("bound session lost its password-change restriction")
	}
	if grants, err := f.db.ResolveBoundSessionGrants(ctx, record.TokenHash, binding, "", f.clock(), nil); err != session.ErrPasswordChangeRequired || len(grants) != 0 {
		t.Fatal("password-restricted bound session received grants")
	}
}

func TestBoundSessionQueuedExpiryAndClockFailure(t *testing.T) {
	for _, operation := range []string{"use", "grants"} {
		t.Run(operation+"/queued-expiry", func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			record, _ := browserSessionRecord("queued-"+operation, "browser-local-a", binding, f.clock())
			if _, err := f.db.CreateLocalSession(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			requested := f.clock()
			err := runQueuedDatabaseOperation(t, f.db, func() {
				f.current.Store(record.IdleExpiresAt.UnixNano())
			}, func() error {
				if operation == "use" {
					_, err := f.db.UseBoundSession(context.Background(), record.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil)
					return err
				}
				_, err := f.db.ResolveBoundSessionGrants(context.Background(), record.TokenHash, binding, "", requested, nil)
				return err
			})
			if err != session.ErrInvalidSession {
				t.Fatalf("queued expired bound session was not rejected: %v", err)
			}
		})
	}
	for _, mode := range []string{"backwards", "expiry"} {
		for _, stage := range []int{2, 3} {
			t.Run(fmt.Sprintf("use/%s-%d", mode, stage), func(t *testing.T) {
				f, binding, _ := newBrowserSessionFixture(t)
				record, _ := browserSessionRecord("clock-use", "browser-local-a", binding, f.clock())
				if _, err := f.db.CreateLocalSession(context.Background(), record); err != nil {
					t.Fatal(err)
				}
				calls := 0
				clock := func() time.Time {
					calls++
					if calls >= stage {
						if mode == "backwards" {
							return record.CreatedAt.Add(-time.Nanosecond)
						}
						return record.AbsoluteExpiresAt
					}
					return record.CreatedAt
				}
				principal, err := f.db.UseBoundSession(context.Background(), record.TokenHash, binding, clock, session.DefaultIdleLifetime, nil)
				want := session.ErrInvalidSession
				if mode == "backwards" {
					want = session.ErrInvalidSessionConfig
				}
				if err != want || principal.SessionID != "" {
					t.Fatal("invalid clock or expired deadline returned a bound principal")
				}
			})
		}
		for _, stage := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("grants/%s-%d", mode, stage), func(t *testing.T) {
				f, binding, _ := newBrowserSessionFixture(t)
				record, _ := browserSessionRecord("clock-grants", "browser-local-a", binding, f.clock())
				if _, err := f.db.CreateLocalSession(context.Background(), record); err != nil {
					t.Fatal(err)
				}
				calls := 0
				f.db.now = func() time.Time {
					calls++
					if calls >= stage {
						if mode == "backwards" {
							return record.CreatedAt.Add(-time.Nanosecond)
						}
						return record.IdleExpiresAt
					}
					return record.CreatedAt
				}
				grants, err := f.db.ResolveBoundSessionGrants(context.Background(), record.TokenHash, binding, "", record.CreatedAt, nil)
				want := session.ErrInvalidSession
				if mode == "backwards" {
					want = ErrInvalidAuditEvent
				}
				if err != want || len(grants) != 0 {
					t.Fatal("invalid clock or expired deadline returned bound grants")
				}
			})
		}
	}
	t.Run("clock-before-session-creation", func(t *testing.T) {
		f, binding, _ := newBrowserSessionFixture(t)
		record, _ := browserSessionRecord("clock-before-creation", "browser-local-a", binding, f.clock())
		if _, err := f.db.CreateLocalSession(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		f.current.Store(record.CreatedAt.Add(-time.Nanosecond).UnixNano())
		if principal, err := f.db.UseBoundSession(context.Background(), record.TokenHash, binding, f.clock, session.DefaultIdleLifetime, nil); err != session.ErrInvalidSessionConfig || principal.SessionID != "" {
			t.Fatal("bound session resolved before its creation time")
		}
		if grants, err := f.db.ResolveBoundSessionGrants(context.Background(), record.TokenHash, binding, "", f.clock(), nil); !errors.Is(err, ErrInvalidAuditEvent) || len(grants) != 0 {
			t.Fatal("bound grants resolved before session creation")
		}
	})
}

func TestBoundSessionRejectsFutureCognitoClaims(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	record, _, protector := browserCognitoRecord(t, f, "future-claims", binding)
	if _, err := f.db.CreateCognitoSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET issued_at_ns = ? WHERE session_id = ?",
		f.clock().Add(time.Second).UnixNano(), record.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.db.UseBoundSession(ctx, record.Session.TokenHash, binding, f.clock, session.DefaultIdleLifetime, protector); err != session.ErrInvalidSessionConfig || principal.SessionID != "" {
		t.Fatal("future Cognito claims resolved a bound session")
	}
	if grants, err := f.db.ResolveBoundSessionGrants(ctx, record.Session.TokenHash, binding, "", f.clock(), protector); !errors.Is(err, ErrInvalidAuditEvent) || len(grants) != 0 {
		t.Fatal("future Cognito claims returned bound grants")
	}
}

func TestBoundSessionCognitoTamperedCredentialFailsClosed(t *testing.T) {
	f, binding, _ := newBrowserSessionFixture(t)
	ctx := context.Background()
	record, _, protector := browserCognitoRecord(t, f, "tampered", binding)
	if _, err := f.db.CreateCognitoSession(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.Exec("UPDATE cognito_session_credentials SET encrypted_refresh_token = zeroblob(32) WHERE session_id = ?", record.Session.SessionID); err != nil {
		t.Fatal(err)
	}
	if principal, err := f.db.UseBoundSession(ctx, record.Session.TokenHash, binding, f.clock, session.DefaultIdleLifetime, protector); err == nil || err == session.ErrInvalidSession || principal.SessionID != "" {
		t.Fatal("tampered Cognito credential was hidden or accepted")
	}
	if grants, err := f.db.ResolveBoundSessionGrants(ctx, record.Session.TokenHash, binding, "", f.clock(), protector); !errors.Is(err, session.ErrCognitoRefreshUnavailable) || len(grants) != 0 {
		t.Fatal("tampered Cognito credential returned grants")
	}
	if f.refreshCalls.Load() != 0 {
		t.Fatal("tampered bound session dispatched a provider refresh")
	}
}
