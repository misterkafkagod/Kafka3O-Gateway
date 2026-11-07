package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/cognito"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type boundManagerGrantHookStore struct {
	*Database
	entered chan struct{}
	release chan struct{}
}

func (s *boundManagerGrantHookStore) ResolveBoundSessionGrants(ctx context.Context, tokenHash, browserHash [32]byte, clusterID string,
	now time.Time, protector *cognito.RefreshTokenProtector) ([]authorization.Grant, error) {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Database.ResolveBoundSessionGrants(ctx, tokenHash, browserHash, clusterID, now, protector)
}

func TestBoundSessionManagerGrantsRecheckChangesBetweenValidationAndDecision(t *testing.T) {
	for _, mode := range []string{"replacement", "revocation", "grant-removal", "password-change", "missing-binding-storage"} {
		t.Run(mode, func(t *testing.T) {
			f, binding, _ := newBrowserSessionFixture(t)
			ctx := context.Background()
			record, token := browserSessionRecord("grant-race-"+mode, "browser-local-a", binding, f.clock())
			if _, err := f.db.CreateLocalSession(ctx, record); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.db.Exec("INSERT INTO role_grants (account_id, cluster_id, role) VALUES (?, 'cluster-a', 'ADMIN')", record.AccountID); err != nil {
				t.Fatal(err)
			}
			store := &boundManagerGrantHookStore{Database: f.db, entered: make(chan struct{}), release: make(chan struct{})}
			manager, err := session.NewManager(store, rand.Reader, f.clock, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				select {
				case <-store.release:
				default:
					close(store.release)
				}
			})
			type resolution struct {
				principal session.Principal
				grants    []authorization.Grant
				err       error
			}
			done := make(chan resolution, 1)
			go func() {
				principal, grants, err := manager.ResolveBoundGrants(ctx, token, binding, "cluster-a")
				done <- resolution{principal: principal, grants: grants, err: err}
			}()
			select {
			case <-store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("bound grant resolution did not reach its decision check")
			}
			switch mode {
			case "replacement":
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 0 WHERE id = ?", record.AccountID); err != nil {
					t.Fatal(err)
				}
				replacement, _ := browserSessionRecord("grant-replacement", "browser-local-b", binding, f.clock())
				if _, err := f.db.CreateLocalSession(ctx, replacement); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.db.Exec("UPDATE identities SET enabled = 1 WHERE id = ?", record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "revocation":
				if err := f.db.RevokeBoundSession(ctx, record.TokenHash, binding, "revoke-bound-grant", f.clock()); err != nil {
					t.Fatal(err)
				}
			case "grant-removal":
				if _, err := f.db.db.Exec("DELETE FROM role_grants WHERE account_id = ?", record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "password-change":
				if _, err := f.db.db.Exec("UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", record.AccountID); err != nil {
					t.Fatal(err)
				}
			case "missing-binding-storage":
				if _, err := f.db.db.Exec("DROP TABLE console_browser_sessions"); err != nil {
					t.Fatal(err)
				}
			}
			close(store.release)
			select {
			case result := <-done:
				switch mode {
				case "replacement", "revocation":
					if result.err != session.ErrInvalidSession || result.principal.SessionID != "" || len(result.grants) != 0 {
						t.Fatalf("stale bound principal received grants: %v", result.err)
					}
				case "grant-removal":
					if result.err != nil || result.principal.SessionID != record.SessionID || len(result.grants) != 0 {
						t.Fatalf("removed grant remained effective: %v", result.err)
					}
				case "password-change":
					if result.err != session.ErrPasswordChangeRequired || result.principal.SessionID != "" || len(result.grants) != 0 {
						t.Fatalf("changed password restriction was bypassed: %v", result.err)
					}
				case "missing-binding-storage":
					if result.err == nil || errors.Is(result.err, session.ErrInvalidSession) || result.principal.SessionID != "" || len(result.grants) != 0 {
						t.Fatalf("binding storage failure was hidden: %v", result.err)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("bound grant resolution did not finish")
			}
		})
	}
}
