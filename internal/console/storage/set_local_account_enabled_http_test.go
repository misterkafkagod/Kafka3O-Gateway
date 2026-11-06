package storage

import (
	"context"
	"crypto/rand"
	"encoding/json"
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

func accountStateTestHandler(t *testing.T, f accountAdministrationFixture, setEnabled identity.SetLocalAccountEnabled, listOverride ...identity.ListLocalAccounts) http.Handler {
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
	list, err := identity.NewListLocalAccounts(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if len(listOverride) != 0 {
		list = listOverride[0]
	}
	listSessions, err := identity.NewListAdminSessions(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	revokeAdmin, err := identity.NewRevokeAdminSession(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewSessionAPI(login, change, create, reset, setEnabled, list, listSessions, revokeAdmin, f.manager, "https://console.example",
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func accountStateTestRequest(handler http.Handler, credentials session.Credentials, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "https://console.example/api/admin/local-accounts/"+target+"/enabled", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://console.example")
	req.Header.Set(session.CSRFHeader, credentials.CSRFToken)
	req.AddCookie(&http.Cookie{Name: session.CookieName, Value: credentials.Token})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func TestAccountStateHTTPSelfDisableRevokesSession(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, secondSession := f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
	handler := accountStateTestHandler(t, f, f.change)
	disabled := accountStateTestRequest(handler, f.adminRun, f.admin.ID, `{"enabled":false}`)
	var result struct {
		OperationID    string `json:"operationId"`
		Enabled        bool   `json:"enabled"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if disabled.Code != http.StatusOK || json.Unmarshal(disabled.Body.Bytes(), &result) != nil ||
		result.OperationID == "" || result.Enabled || !result.SignInRequired {
		t.Fatalf("self-disable status=%d", disabled.Code)
	}
	cleared := map[string]bool{}
	for _, cookie := range disabled.Result().Cookies() {
		if cookie.MaxAge < 0 {
			cleared[cookie.Name] = true
		}
	}
	if !cleared[session.CookieName] || !cleared[session.CSRFCookieName] {
		t.Fatal("self-disable did not clear browser cookies")
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("self-disabled administrator session remained valid: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, secondSession.Token); err != nil {
		t.Fatalf("self-disable revoked the other administrator: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
	if err != nil || len(events) != 2 || events[1].OperationID != result.OperationID ||
		events[1].ActorID != f.admin.ID || events[1].ResourceID != f.admin.ID || events[1].Outcome != AuditSucceeded {
		t.Fatalf("self-disable audit count=%d err=%v", len(events), err)
	}
	enabled := accountStateTestRequest(handler, secondSession, f.admin.ID, `{"enabled":true}`)
	if enabled.Code != http.StatusOK {
		t.Fatalf("second administrator re-enable status=%d", enabled.Code)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("re-enabling revived the old administrator session: %v", err)
	}
	if err := f.db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("enabled administrators unavailable after re-enable: %v", err)
	}
}

type accountStateBefore struct {
	before func(context.Context, identity.LocalAdminActor) error
	next   identity.SetLocalAccountEnabled
}

func (s accountStateBefore) Execute(ctx context.Context, actor identity.LocalAdminActor, target string, enabled bool) (identity.LocalAccountStateResult, error) {
	if err := s.before(ctx, actor); err != nil {
		return identity.LocalAccountStateResult{}, err
	}
	return s.next.Execute(ctx, actor, target, enabled)
}

func TestAccountStateHTTPRechecksAuthorizationAfterSessionResolution(t *testing.T) {
	for _, kind := range []string{"demoted", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
			f.addAccount(t, "second-admin-id", "second-admin", "", "ADMIN")
			wrapper := accountStateBefore{
				next: f.change,
				before: func(ctx context.Context, actor identity.LocalAdminActor) error {
					if kind == "revoked" {
						return f.manager.Revoke(ctx, f.adminRun.Token)
					}
					_, err := f.db.db.ExecContext(ctx, "DELETE FROM role_grants WHERE account_id = ? AND cluster_id = ''", actor.AccountID)
					return err
				},
			}
			handler := accountStateTestHandler(t, f, wrapper)
			denied := accountStateTestRequest(handler, f.adminRun, target.ID, `{"enabled":false}`)
			wantStatus, wantCode := http.StatusForbidden, "global_admin_required"
			if kind == "revoked" {
				wantStatus, wantCode = http.StatusUnauthorized, "unauthorized"
			}
			if denied.Code != wantStatus || !strings.Contains(denied.Body.String(), `"code":"`+wantCode+`"`) {
				t.Fatalf("%s administrator changed state: status=%d", kind, denied.Code)
			}
			stored, err := f.db.FindLocalAccount(ctx, target.Username)
			if err != nil || !stored.Enabled || stored.CredentialVersion != target.CredentialVersion {
				t.Fatalf("denied request changed the target: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
				t.Fatalf("denied request revoked target session: %v", err)
			}
			events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 10})
			if err != nil || len(events) != 2 || events[1].Outcome != AuditDenied {
				t.Fatalf("%s administrator denial not audited: count=%d err=%v", kind, len(events), err)
			}
		})
	}
}

func TestAccountStateHTTPFailsClosedWhenAuditUnavailable(t *testing.T) {
	for _, condition := range []string{
		"NEW.action = 'console.local_account.disable' AND NEW.outcome = 'attempted'",
		"NEW.action = 'console.local_account.disable' AND NEW.outcome = 'succeeded'",
		"NEW.action = 'console.sessions.revoke_on_account_state_change'",
	} {
		t.Run(condition, func(t *testing.T) {
			f := newAccountAdministrationFixture(t)
			ctx := context.Background()
			target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
			_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_state_audit BEFORE INSERT ON audit_events WHEN `+condition+`
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
			if err != nil {
				t.Fatal(err)
			}
			handler := accountStateTestHandler(t, f, f.change)
			response := accountStateTestRequest(handler, f.adminRun, target.ID, `{"enabled":false}`)
			if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"account_state_audit_unavailable"`) ||
				!strings.Contains(response.Body.String(), `"operationId":`) {
				t.Fatalf("audit outage status=%d", response.Code)
			}
			stored, err := f.db.FindLocalAccount(ctx, target.Username)
			if err != nil || !stored.Enabled || stored.CredentialVersion != target.CredentialVersion {
				t.Fatalf("audit outage changed account state: %v", err)
			}
			if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
				t.Fatalf("audit outage revoked target session: %v", err)
			}
		})
	}
}

func TestAccountStateHTTPAuditFailureTakesPrecedenceOverDenial(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	_, err := f.db.db.ExecContext(ctx, `CREATE TRIGGER reject_state_denial BEFORE INSERT ON audit_events
WHEN NEW.action = 'console.local_account.disable' AND NEW.outcome = 'denied'
BEGIN SELECT RAISE(FAIL, 'audit unavailable'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	handler := accountStateTestHandler(t, f, f.change)
	response := accountStateTestRequest(handler, f.adminRun, f.admin.ID, `{"enabled":false}`)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"account_state_audit_unavailable"`) {
		t.Fatalf("unaudited last-admin denial status=%d", response.Code)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); err != nil {
		t.Fatalf("unaudited denial revoked the administrator session: %v", err)
	}
	if err := f.db.VerifyInitialAdministrator(ctx); err != nil {
		t.Fatalf("unaudited denial changed the administrator: %v", err)
	}
}
