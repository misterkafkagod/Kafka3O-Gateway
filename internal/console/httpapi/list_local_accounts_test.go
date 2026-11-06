package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func TestAdministratorAccountListingHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	response, _ := c.request(t, http.MethodGet, "/api/admin/local-accounts", "", "", nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous account listing status=%d", response.StatusCode)
	}
	_, adminCookies, adminCSRF, restricted := c.login(t, "admin", "initial passphrase")
	if !restricted {
		t.Fatal("bootstrap administrator was not restricted")
	}
	response, _ = c.request(t, http.MethodGet, "/api/admin/local-accounts", "", "", adminCookies, "")
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted administrator listed accounts: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("administrator password change status=%d", response.StatusCode)
	}
	adminID, adminCookies, adminCSRF, _ := c.login(t, "admin", "replacement passphrase")
	ids := map[string]string{}
	for _, username := range []string{"bob", "charlie", "alice"} {
		body := fmt.Sprintf(`{"username":%q,"temporaryPassword":"user temporary passphrase"}`, username)
		response, data := c.request(t, http.MethodPost, "/api/admin/local-accounts", body, c.origin, adminCookies, adminCSRF)
		var created struct {
			Account struct {
				ID string `json:"id"`
			} `json:"account"`
		}
		if response.StatusCode != http.StatusCreated || json.Unmarshal(data, &created) != nil || created.Account.ID == "" {
			t.Fatalf("create %s status=%d", username, response.StatusCode)
		}
		ids[username] = created.Account.ID
	}
	charliePath := "/api/admin/local-accounts/" + ids["charlie"] + "/enabled"
	response, _ = c.request(t, http.MethodPut, charliePath, `{"enabled":false}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("disable account status=%d", response.StatusCode)
	}
	for _, query := range []string{
		"?limit=0", "?limit=101", "?limit=-1", "?limit=not-a-number", "?limit=1&limit=2",
		"?unknown=value", "?after=", "?after=one&after=two", "?after=%20leading", "?after=%00", "?after=%FF",
	} {
		response, _ := c.request(t, http.MethodGet, "/api/admin/local-accounts"+query, "", "", adminCookies, "")
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid account list query %q status=%d", query, response.StatusCode)
		}
	}
	response, data := c.request(t, http.MethodGet, "/api/admin/local-accounts?after=unknown-sensitive-cursor", "", "", adminCookies, "")
	if response.StatusCode != http.StatusBadRequest || !bytes.Contains(data, []byte(`"code":"invalid_account_cursor"`)) ||
		bytes.Contains(data, []byte("unknown-sensitive-cursor")) {
		t.Fatalf("unknown account cursor status=%d", response.StatusCode)
	}
	type accountPage struct {
		OperationID string `json:"operationId"`
		Accounts    []struct {
			ID                 string `json:"id"`
			Username           string `json:"username"`
			Enabled            bool   `json:"enabled"`
			MustChangePassword bool   `json:"mustChangePassword"`
		} `json:"accounts"`
		NextCursor string `json:"nextCursor"`
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/local-accounts?limit=2", "", "", adminCookies, "")
	var first accountPage
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" ||
		json.Unmarshal(data, &first) != nil || first.OperationID == "" || len(first.Accounts) != 2 ||
		first.Accounts[0].ID != adminID || first.Accounts[0].Username != "admin" ||
		first.Accounts[1].ID != ids["alice"] || first.NextCursor != ids["alice"] {
		t.Fatalf("first account page status=%d", response.StatusCode)
	}
	for _, secret := range []string{"passwordHash", "credentialVersion", "user temporary passphrase", adminCSRF} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatal("account listing exposed a credential")
		}
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/local-accounts?limit=2&after="+first.NextCursor, "", "", adminCookies, "")
	var second accountPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &second) != nil || second.OperationID == "" ||
		second.OperationID == first.OperationID || len(second.Accounts) != 2 ||
		second.Accounts[0].ID != ids["bob"] || !second.Accounts[0].Enabled || !second.Accounts[0].MustChangePassword ||
		second.Accounts[1].ID != ids["charlie"] || second.Accounts[1].Enabled || second.NextCursor != "" {
		t.Fatalf("second account page status=%d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/local-accounts?after="+ids["charlie"], "", "", adminCookies, "")
	var empty accountPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &empty) != nil || empty.OperationID == "" ||
		empty.Accounts == nil || len(empty.Accounts) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty account page status=%d", response.StatusCode)
	}
	_, userCookies, userCSRF, restricted := c.login(t, "alice", "user temporary passphrase")
	if !restricted {
		t.Fatal("created user did not require a password change")
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"user temporary passphrase","newPassword":"user replacement passphrase"}`, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("user password change status=%d", response.StatusCode)
	}
	_, userCookies, _, restricted = c.login(t, "alice", "user replacement passphrase")
	if restricted {
		t.Fatal("user remained restricted")
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/local-accounts", "", "", userCookies, "")
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) ||
		bytes.Contains(data, []byte(`"accounts":`)) {
		t.Fatalf("role-free user account listing status=%d", response.StatusCode)
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.list", Limit: 10})
	if err != nil || len(events) != 5 || events[0].Outcome != storage.AuditValidationRejected ||
		events[1].OperationID != first.OperationID || events[2].OperationID != second.OperationID ||
		events[3].OperationID != empty.OperationID ||
		events[4].ActorID != ids["alice"] || events[4].Outcome != storage.AuditDenied {
		t.Fatalf("account listing audit count=%d err=%v", len(events), err)
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, secret := range []string{"unknown-sensitive-cursor", "alice", "bob", "charlie", "user temporary passphrase", adminCSRF, userCSRF} {
		if strings.Contains(serialized, secret) {
			t.Fatal("account listing audit contained untrusted input or a credential")
		}
	}
}

func TestAccountListingHTTPRejectsMalformedQueriesBeforeStore(t *testing.T) {
	list := &capturedAccountList{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
		unusedAccountState{}, list, unusedAdminSessionList{}, unusedAdminSessionRevocation{}, resetTestSessions{principal: session.Principal{AccountID: "admin", SessionID: "session"}},
		"https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"limit=%zz", "after=%", "limit=1;after=user"} {
		request := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/local-accounts", nil)
		request.URL.RawQuery = query
		request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"invalid_request"`) ||
			list.calls != 0 {
			t.Fatalf("malformed query reached the store or returned the wrong status: %d", recorder.Code)
		}
	}
}

type capturedAccountList struct {
	actor  identity.LocalAdminActor
	after  string
	limit  int
	result identity.LocalAccountListResult
	err    error
	calls  int
}

func (s *capturedAccountList) Execute(_ context.Context, actor identity.LocalAdminActor, after string, limit int) (identity.LocalAccountListResult, error) {
	s.calls++
	s.actor, s.after, s.limit = actor, after, limit
	return s.result, s.err
}

func TestAccountListingHTTPErrorSemanticsAndServerDerivedActor(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
	}{
		{"audit failure with denial", "account_list_audit_unavailable", errors.Join(identity.ErrGlobalAdminRequired, identity.ErrAccountListAudit), http.StatusServiceUnavailable},
		{"unexpected failure", "account_list_unavailable", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized},
		{"non-admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden},
		{"password change required", "password_change_required", identity.ErrAdminPasswordChange, http.StatusForbidden},
		{"invalid list", "invalid_account_list", identity.ErrInvalidAccountList, http.StatusBadRequest},
		{"invalid cursor", "invalid_account_cursor", identity.ErrInvalidAccountCursor, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			principal := session.Principal{AccountID: "server-admin", SessionID: "server-session"}
			list := &capturedAccountList{result: identity.LocalAccountListResult{
				OperationID: "operation-123", Accounts: []identity.LocalAccountSummary{{ID: "user", Username: "sensitive-user"}},
			}, err: tt.err}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				unusedAccountState{}, list, unusedAdminSessionList{}, unusedAdminSessionRevocation{}, resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/local-accounts?limit=2&after=cursor", nil)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.status || !strings.Contains(recorder.Body.String(), `"code":"`+tt.code+`"`) ||
				!strings.Contains(recorder.Body.String(), `"operationId":"operation-123"`) {
				t.Fatalf("account listing error status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if list.calls != 1 || list.actor.AccountID != principal.AccountID || list.actor.SessionID != principal.SessionID ||
				list.after != "cursor" || list.limit != 2 {
				t.Fatal("account listing actor was not derived from the authenticated session")
			}
			for _, secret := range []string{"internal-sensitive-value", "opaque-cookie", "sensitive-user"} {
				if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
					t.Fatal("account listing response or logs exposed an internal error or account")
				}
			}
			if tt.status == http.StatusServiceUnavailable && !strings.Contains(logs.String(), "error_type") {
				t.Fatal("account listing service failure was not logged")
			}
		})
	}
}

func TestAccountListingHTTPRejectsInvalidSuccessResults(t *testing.T) {
	valid := identity.LocalAccountSummary{ID: "user-1", Username: "user-a", Enabled: true}
	for _, tt := range []struct {
		name   string
		result identity.LocalAccountListResult
	}{
		{"missing operation", identity.LocalAccountListResult{Accounts: []identity.LocalAccountSummary{valid}}},
		{"nil accounts", identity.LocalAccountListResult{OperationID: "operation"}},
		{"missing ID", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{{Username: "user-a"}}}},
		{"control in ID", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{{ID: "user\x00", Username: "user-a"}}}},
		{"un-normalized username", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{{ID: "user", Username: "User-A"}}}},
		{"unsorted usernames", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{
			{ID: "user-2", Username: "user-b"}, valid,
		}}},
		{"duplicate ID", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{
			valid, {ID: "user-1", Username: "user-b"},
		}}},
		{"too many accounts", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{
			valid, {ID: "user-2", Username: "user-b"}, {ID: "user-3", Username: "user-c"},
		}}},
		{"invalid next cursor", identity.LocalAccountListResult{OperationID: "operation", Accounts: []identity.LocalAccountSummary{valid}, NextCursor: "wrong"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			list := &capturedAccountList{result: tt.result}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				unusedAccountState{}, list, unusedAdminSessionList{}, unusedAdminSessionRevocation{}, resetTestSessions{principal: session.Principal{AccountID: "admin", SessionID: "session"}},
				"https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/local-accounts?limit=2", nil)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"account_list_unavailable"`) ||
				!strings.Contains(logs.String(), "invalid result") || strings.Contains(recorder.Body.String(), "user-a") {
				t.Fatalf("invalid listing was reported as success: status=%d", recorder.Code)
			}
		})
	}
}

func TestSessionAPIRequiresAccountListService(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
		unusedAccountState{}, nil, unusedAdminSessionList{}, unusedAdminSessionRevocation{}, failedSessions{}, "https://console.example", logger)
	if !errors.Is(err, consoleapi.ErrInvalidAPIConfig) {
		t.Fatalf("missing account list service was accepted: %v", err)
	}
}
