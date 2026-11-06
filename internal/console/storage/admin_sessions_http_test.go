package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func adminSessionsTestHandler(t *testing.T, f accountAdministrationFixture, list identity.ListAdminSessions, revoke identity.RevokeAdminSession, logs io.Writer) http.Handler {
	t.Helper()
	authenticator, err := identity.NewAuthenticateLocal(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	login, err := session.NewLocalLogin(authenticator, f.manager)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewChangeLocalPassword(authenticator, f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	create, err := identity.NewCreateLocalAccount(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	listAccounts, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewSessionAPI(login, change, create, reset, f.change, listAccounts, list, revoke, f.manager,
		"https://console.example", slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func adminSessionsTestRequest(handler http.Handler, credentials session.Credentials, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "https://console.example"+path, nil)
	if method == http.MethodPost {
		req.Header.Set("Origin", "https://console.example")
		req.Header.Set(session.CSRFHeader, credentials.CSRFToken)
	}
	req.AddCookie(&http.Cookie{Name: session.CookieName, Value: credentials.Token})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

type adminSessionListBefore struct {
	before func(context.Context, identity.LocalAdminActor) error
	next   identity.ListAdminSessions
}

func (s adminSessionListBefore) Execute(ctx context.Context, actor identity.LocalAdminActor, after string, limit int) (identity.AdminSessionListResult, error) {
	if err := s.before(ctx, actor); err != nil {
		return identity.AdminSessionListResult{}, err
	}
	return s.next.Execute(ctx, actor, after, limit)
}

type adminSessionRevocationBefore struct {
	before func(context.Context, identity.LocalAdminActor) error
	next   identity.RevokeAdminSession
}

func (s adminSessionRevocationBefore) Execute(ctx context.Context, actor identity.LocalAdminActor, target string) (identity.AdminSessionRevocationResult, error) {
	if err := s.before(ctx, actor); err != nil {
		return identity.AdminSessionRevocationResult{}, err
	}
	return s.next.Execute(ctx, actor, target)
}

func TestAdminSessionHTTPRechecksAuthorizationAfterSessionResolution(t *testing.T) {
	for _, operation := range []string{"list", "revoke"} {
		for _, kind := range []string{"demoted", "restricted", "revoked", "expired"} {
			t.Run(operation+"/"+kind, func(t *testing.T) {
				f := newAccountAdministrationFixture(t)
				ctx := context.Background()
				_, target := f.addAccount(t, "subject-1", "other-user", "", "")
				list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				before := func(ctx context.Context, actor identity.LocalAdminActor) error {
					switch kind {
					case "demoted":
						_, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", actor.AccountID)
						return err
					case "restricted":
						_, err := f.db.db.ExecContext(ctx, "UPDATE local_accounts SET must_change_password = 1 WHERE identity_id = ?", actor.AccountID)
						return err
					case "revoked":
						return f.manager.Revoke(ctx, f.adminRun.Token)
					default:
						_, err := f.db.db.ExecContext(ctx, "UPDATE console_sessions SET idle_expires_at_ns = created_at_ns + 1 WHERE id = ?", actor.SessionID)
						return err
					}
				}
				method, path, action, eventCount := http.MethodGet, "/api/admin/sessions", "console.session.list", 1
				if operation == "list" {
					list = adminSessionListBefore{before: before, next: list}
				} else {
					revoke = adminSessionRevocationBefore{before: before, next: revoke}
					method, path, action, eventCount = http.MethodPost, "/api/admin/sessions/"+target.Principal.SessionID+"/revoke", "console.session.admin_revoke", 2
				}
				handler := adminSessionsTestHandler(t, f, list, revoke, io.Discard)
				response := adminSessionsTestRequest(handler, f.adminRun, method, path)
				wantStatus, wantCode := http.StatusForbidden, "global_admin_required"
				if kind == "restricted" {
					wantCode = "password_change_required"
				} else if kind == "revoked" || kind == "expired" {
					wantStatus, wantCode = http.StatusUnauthorized, "unauthorized"
				}
				if response.Code != wantStatus || !strings.Contains(response.Body.String(), `"code":"`+wantCode+`"`) ||
					strings.Contains(response.Body.String(), `"sessions":`) || strings.Contains(response.Body.String(), target.Principal.SessionID) {
					t.Fatalf("%s %s administrator status=%d", operation, kind, response.Code)
				}
				if _, err := f.manager.Resolve(ctx, target.Token); err != nil {
					t.Fatalf("denied operation revoked target: %v", err)
				}
				events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: action, Limit: 10})
				if err != nil || len(events) != eventCount || events[len(events)-1].Outcome != AuditDenied ||
					events[len(events)-1].ResourceID != "" {
					t.Fatalf("%s denial audit count=%d err=%v", operation, len(events), err)
				}
			})
		}
	}
}

func TestAdminSessionHTTPRejectsClusterOnlyAndRoleFreeUsers(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, clusterAdmin := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
	_, roleFree := f.addAccount(t, "role-free-id", "role-free", "", "")
	_, target := f.addAccount(t, "subject-1", "other-user", "", "")
	list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	handler := adminSessionsTestHandler(t, f, list, revoke, io.Discard)
	for _, actor := range []session.Credentials{clusterAdmin, roleFree} {
		for _, operation := range []struct{ method, path string }{
			{http.MethodGet, "/api/admin/sessions"},
			{http.MethodPost, "/api/admin/sessions/" + target.Principal.SessionID + "/revoke"},
		} {
			response := adminSessionsTestRequest(handler, actor, operation.method, operation.path)
			if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), `"code":"global_admin_required"`) ||
				strings.Contains(response.Body.String(), `"sessions":`) {
				t.Fatalf("non-global administrator received status=%d", response.Code)
			}
		}
	}
	if _, err := f.manager.Resolve(ctx, target.Token); err != nil {
		t.Fatalf("denied request revoked target: %v", err)
	}
}

func TestAdminSessionHTTPFailsClosedWhenAuditUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name, operation, condition, code string
	}{
		{"listing", "list", "NEW.action = 'console.session.list'", "session_list_audit_unavailable"},
		{"revocation attempt", "revoke", "NEW.action = 'console.session.admin_revoke' AND NEW.outcome = 'attempted'", "session_revoke_audit_unavailable"},
		{"revocation outcome", "revoke", "NEW.action = 'console.session.admin_revoke' AND NEW.outcome = 'succeeded'", "session_revoke_audit_unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			_, target := f.addAccount(t, "subject-1", "other-user", "", "")
			if _, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_admin_session_audit BEFORE INSERT ON audit_events WHEN `+tt.condition+`
BEGIN SELECT RAISE(FAIL, 'internal-sensitive-audit-failure'); END;`); err != nil {
				t.Fatal(err)
			}
			list, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			revoke, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			var logs bytes.Buffer
			handler := adminSessionsTestHandler(t, f, list, revoke, &logs)
			method, path := http.MethodGet, "/api/admin/sessions"
			if tt.operation == "revoke" {
				method, path = http.MethodPost, "/api/admin/sessions/"+target.Principal.SessionID+"/revoke"
			}
			response := adminSessionsTestRequest(handler, f.adminRun, method, path)
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"`+tt.code+`"`) ||
				!strings.Contains(response.Body.String(), `"operationId":`) || strings.Contains(response.Body.String(), `"sessions":`) ||
				!strings.Contains(logs.String(), "error_type") {
				t.Fatalf("audit outage status=%d", response.Code)
			}
			for _, secret := range []string{"internal-sensitive-audit-failure", f.adminRun.Token, f.adminRun.CSRFToken, target.Token, target.CSRFToken} {
				if strings.Contains(logs.String(), secret) || strings.Contains(response.Body.String(), secret) {
					t.Fatal("audit outage exposed an internal error or credential")
				}
			}
			if _, err := f.manager.Resolve(ctx, target.Token); err != nil && !errors.Is(err, session.ErrInvalidSession) {
				t.Fatalf("target session unavailable after audit failure: %v", err)
			} else if err != nil {
				t.Fatal("audit failure revoked the target session")
			}
		})
	}
}
