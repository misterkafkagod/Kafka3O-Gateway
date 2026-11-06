package storage

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type accountListBefore struct {
	before func(context.Context, identity.LocalAdminActor) error
	next   identity.ListLocalAccounts
}

func (s accountListBefore) Execute(ctx context.Context, actor identity.LocalAdminActor, after string, limit int) (identity.LocalAccountListResult, error) {
	if err := s.before(ctx, actor); err != nil {
		return identity.LocalAccountListResult{}, err
	}
	return s.next.Execute(ctx, actor, after, limit)
}

func accountListTestRequest(handler http.Handler, credentials session.Credentials) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/local-accounts?limit=2", nil)
	req.AddCookie(&http.Cookie{Name: session.CookieName, Value: credentials.Token})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func TestAccountListingHTTPRechecksAuthorizationAfterSessionResolution(t *testing.T) {
	for _, kind := range []string{"demoted", "revoked", "expired", "password change"} {
		t.Run(kind, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
			list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			wrapper := accountListBefore{
				next: list,
				before: func(ctx context.Context, actor identity.LocalAdminActor) error {
					switch kind {
					case "revoked":
						return f.manager.Revoke(ctx, f.adminRun.Token)
					case "expired":
						_, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = created_at_ns + 1 WHERE id = ?",
							actor.SessionID)
						return err
					case "password change":
						_, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", actor.AccountID)
						return err
					default:
						_, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", actor.AccountID)
						return err
					}
				},
			}
			handler := accountStateTestHandler(t, f, f.change, wrapper)
			denied := accountListTestRequest(handler, f.adminRun)
			wantStatus, wantCode := http.StatusForbidden, "global_admin_required"
			if kind == "revoked" || kind == "expired" {
				wantStatus, wantCode = http.StatusUnauthorized, "unauthorized"
			} else if kind == "password change" {
				wantCode = "password_change_required"
			}
			if denied.Code != wantStatus || !strings.Contains(denied.Body.String(), `"code":"`+wantCode+`"`) ||
				strings.Contains(denied.Body.String(), `"accounts":`) {
				t.Fatalf("%s administrator listed accounts: status=%d", kind, denied.Code)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
			if err != nil || len(events) != 1 || events[0].Outcome != AuditDenied {
				t.Fatalf("%s listing denial not audited: count=%d err=%v", kind, len(events), err)
			}
		})
	}
}

func TestAccountListingHTTPFailsClosedWhenAuditUnavailable(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_list_audit BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_account.list'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	handler := accountStateTestHandler(t, f, f.change)
	response := accountListTestRequest(handler, f.adminRun)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"account_list_audit_unavailable"`) ||
		strings.Contains(response.Body.String(), `"accounts":`) || strings.Contains(response.Body.String(), `"admin"`) {
		t.Fatalf("audit outage returned account data: status=%d", response.Code)
	}
}
