package httpapi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type adminSessionPage struct {
	OperationID string `json:"operationId"`
	Sessions    []struct {
		ID                string `json:"id"`
		AccountID         string `json:"accountId"`
		Username          string `json:"username"`
		CreatedAt         string `json:"createdAt"`
		IdleExpiresAt     string `json:"idleExpiresAt"`
		AbsoluteExpiresAt string `json:"absoluteExpiresAt"`
		IsCurrent         bool   `json:"isCurrent"`
	} `json:"sessions"`
	NextCursor string `json:"nextCursor"`
}

func TestAdministratorSessionHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	response, _ := c.request(t, http.MethodGet, "/api/admin/sessions", "", "", nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous session listing status=%d", response.StatusCode)
	}
	_, restrictedCookies, restrictedCSRF, restricted := c.login(t, "admin", "initial passphrase")
	if !restricted {
		t.Fatal("bootstrap administrator was not restricted")
	}
	response, _ = c.request(t, http.MethodGet, "/api/admin/sessions", "", "", restrictedCookies, "")
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted administrator listed sessions: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, c.origin, restrictedCookies, restrictedCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("administrator password change status=%d", response.StatusCode)
	}
	adminID, adminCookies, adminCSRF, restricted := c.login(t, "admin", "replacement passphrase")
	if restricted {
		t.Fatal("administrator remained restricted")
	}
	_, otherCookies, otherCSRF, _ := c.login(t, "admin", "replacement passphrase")
	for _, query := range []string{
		"?limit=0", "?limit=101", "?limit=-1", "?limit=not-a-number", "?limit=1&limit=2",
		"?unknown=value", "?after=", "?after=one&after=two", "?after=%20leading", "?after=%00", "?after=%FF",
	} {
		response, _ := c.request(t, http.MethodGet, "/api/admin/sessions"+query, "", "", adminCookies, "")
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid session list query %q status=%d", query, response.StatusCode)
		}
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.session.list", Limit: 10})
	if err != nil || len(events) != 0 {
		t.Fatalf("malformed HTTP queries reached the session list service: count=%d err=%v", len(events), err)
	}
	response, data := c.request(t, http.MethodGet, "/api/admin/sessions?limit=1", "", "", adminCookies, "")
	var first adminSessionPage
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" ||
		json.Unmarshal(data, &first) != nil || first.OperationID == "" || len(first.Sessions) != 1 ||
		first.NextCursor != first.Sessions[0].ID {
		t.Fatalf("first session page status=%d", response.StatusCode)
	}
	for _, cookie := range append(adminCookies, otherCookies...) {
		if cookie.Value != "" && bytes.Contains(data, []byte(cookie.Value)) {
			t.Fatal("session listing exposed a cookie credential")
		}
	}
	for _, secret := range []string{"tokenHash", "csrfHash", "credentialVersion", "passwordHash", adminCSRF, otherCSRF} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatal("session listing exposed credential metadata")
		}
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/sessions?limit=1&after="+first.NextCursor, "", "", adminCookies, "")
	var second adminSessionPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &second) != nil || second.OperationID == "" ||
		second.OperationID == first.OperationID || len(second.Sessions) != 1 || second.NextCursor != "" ||
		second.Sessions[0].ID <= first.Sessions[0].ID {
		t.Fatalf("second session page status=%d", response.StatusCode)
	}
	var currentID, otherID string
	for _, item := range append(first.Sessions, second.Sessions...) {
		if item.AccountID != adminID || item.Username != "admin" {
			t.Fatal("session list returned an unexpected account")
		}
		created, errCreated := time.Parse(time.RFC3339Nano, item.CreatedAt)
		idle, errIdle := time.Parse(time.RFC3339Nano, item.IdleExpiresAt)
		absolute, errAbsolute := time.Parse(time.RFC3339Nano, item.AbsoluteExpiresAt)
		if errCreated != nil || errIdle != nil || errAbsolute != nil || !created.Before(idle) || idle.After(absolute) {
			t.Fatal("session list returned invalid expiry timestamps")
		}
		if item.IsCurrent {
			currentID = item.ID
		} else {
			otherID = item.ID
		}
	}
	if currentID == "" || otherID == "" || currentID == otherID {
		t.Fatal("session list did not distinguish the current session")
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/sessions?after="+second.Sessions[0].ID, "", "", adminCookies, "")
	var empty adminSessionPage
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &empty) != nil || empty.Sessions == nil ||
		len(empty.Sessions) != 0 || empty.NextCursor != "" {
		t.Fatalf("empty session page status=%d", response.StatusCode)
	}

	response, _ = c.request(t, http.MethodPost, "/api/admin/local-accounts",
		`{"username":"example-user","temporaryPassword":"user temporary passphrase"}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create role-free user status=%d", response.StatusCode)
	}
	userID, userCookies, userCSRF, _ := c.login(t, "example-user", "user temporary passphrase")
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"user temporary passphrase","newPassword":"user replacement passphrase"}`, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("user password change status=%d", response.StatusCode)
	}
	_, userCookies, userCSRF, restricted = c.login(t, "example-user", "user replacement passphrase")
	if restricted {
		t.Fatal("user remained restricted")
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/sessions", "", "", userCookies, "")
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) ||
		bytes.Contains(data, []byte(`"sessions":`)) {
		t.Fatalf("role-free user listed sessions: %d", response.StatusCode)
	}
	path := "/api/admin/sessions/" + otherID + "/revoke"
	response, data = c.request(t, http.MethodPost, path, "", c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("role-free user revoked a session: %d", response.StatusCode)
	}
	for _, tt := range []struct {
		name, path, body, origin, csrf string
		cookies                        []*http.Cookie
		want                           int
	}{
		{"anonymous", path, "", c.origin, adminCSRF, nil, http.StatusUnauthorized},
		{"wrong origin", path, "", "https://attacker.example", adminCSRF, adminCookies, http.StatusForbidden},
		{"missing CSRF", path, "", c.origin, "", adminCookies, http.StatusForbidden},
		{"wrong CSRF", path, "", c.origin, "incorrect", adminCookies, http.StatusForbidden},
		{"forged body", path, `{"actorId":"forged"}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"unexpected query", path + "?role=ADMIN", "", c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, _ := c.request(t, http.MethodPost, tt.path, tt.body, tt.origin, tt.cookies, tt.csrf)
			if response.StatusCode != tt.want {
				t.Fatalf("invalid revocation status=%d want=%d", response.StatusCode, tt.want)
			}
		})
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", otherCookies, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("rejected requests revoked the other session: %d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, "/api/admin/sessions/untrusted-input/revoke", "", c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusBadRequest || !bytes.Contains(data, []byte(`"code":"invalid_session"`)) ||
		bytes.Contains(data, []byte("untrusted-input")) {
		t.Fatalf("invalid session target status=%d", response.StatusCode)
	}
	unknown := base64.RawURLEncoding.EncodeToString(make([]byte, 16))
	response, data = c.request(t, http.MethodPost, "/api/admin/sessions/"+unknown+"/revoke", "", c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusNotFound || !bytes.Contains(data, []byte(`"code":"session_unavailable"`)) ||
		bytes.Contains(data, []byte(unknown)) {
		t.Fatalf("unknown session target status=%d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, path, "", c.origin, adminCookies, adminCSRF)
	var otherResult struct {
		OperationID    string `json:"operationId"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" ||
		json.Unmarshal(data, &otherResult) != nil || otherResult.OperationID == "" || otherResult.SignInRequired {
		t.Fatalf("revoke other session status=%d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", otherCookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked session was still accepted: %d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, "/api/admin/sessions/"+currentID+"/revoke", "", c.origin, adminCookies, adminCSRF)
	var selfResult struct {
		OperationID    string `json:"operationId"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &selfResult) != nil ||
		selfResult.OperationID == "" || !selfResult.SignInRequired {
		t.Fatalf("self-revoke status=%d", response.StatusCode)
	}
	cleared := map[string]bool{}
	for _, cookie := range response.Cookies() {
		if cookie.MaxAge < 0 {
			cleared[cookie.Name] = true
		}
	}
	if !cleared[session.CookieName] || !cleared[session.CSRFCookieName] {
		t.Fatal("self-revocation did not clear both browser cookies")
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", adminCookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("self-revoked session was still accepted: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", userCookies, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unrelated user session was revoked: %d", response.StatusCode)
	}
	events, err = c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.session.admin_revoke", Limit: 20})
	if err != nil || len(events) != 10 || events[1].ActorID != userID || events[1].Outcome != storage.AuditDenied ||
		events[7].OperationID != otherResult.OperationID || events[7].ResourceID != otherID ||
		events[9].OperationID != selfResult.OperationID || events[9].ResourceID != currentID {
		t.Fatalf("session revocation audit count=%d err=%v", len(events), err)
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, secret := range []string{"untrusted-input", unknown, adminCSRF, otherCSRF, userCSRF, "user replacement passphrase"} {
		if strings.Contains(serialized, secret) {
			t.Fatal("session revocation audit exposed untrusted input or a credential")
		}
	}
}

type capturedAdminSessionList struct {
	actor  identity.LocalAdminActor
	after  string
	limit  int
	result identity.AdminSessionListResult
	err    error
	calls  int
}

func (s *capturedAdminSessionList) Execute(_ context.Context, actor identity.LocalAdminActor, after string, limit int) (identity.AdminSessionListResult, error) {
	s.calls++
	s.actor, s.after, s.limit = actor, after, limit
	return s.result, s.err
}

func TestAdminSessionListingHTTPRejectsMalformedQueriesBeforeStore(t *testing.T) {
	list := &capturedAdminSessionList{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
		unusedAccountState{}, unusedAccountList{}, list, unusedAdminSessionRevocation{},
		resetTestSessions{principal: session.Principal{AccountID: "admin", SessionID: "session"}}, "https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"limit=%zz", "after=%", "limit=1;after=user", "after=invalid"} {
		request := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/sessions", nil)
		request.URL.RawQuery = query
		request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"invalid_request"`) || list.calls != 0 {
			t.Fatalf("malformed session query reached the store or returned the wrong status: %d", recorder.Code)
		}
	}
}

func TestAdminSessionListingHTTPErrorSemanticsAndServerDerivedActor(t *testing.T) {
	cursor := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16))
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
	}{
		{"audit failure with denial", "session_list_audit_unavailable", errors.Join(identity.ErrGlobalAdminRequired, identity.ErrAdminSessionListAudit), http.StatusServiceUnavailable},
		{"unexpected failure", "session_list_unavailable", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized},
		{"non-admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden},
		{"password change required", "password_change_required", identity.ErrAdminPasswordChange, http.StatusForbidden},
		{"invalid list", "invalid_session_list", identity.ErrInvalidAdminSessionList, http.StatusBadRequest},
		{"invalid cursor", "invalid_session_cursor", identity.ErrInvalidAdminSessionCursor, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			principal := session.Principal{AccountID: "server-admin", SessionID: "server-session"}
			list := &capturedAdminSessionList{result: identity.AdminSessionListResult{
				OperationID: "operation-123", Sessions: []identity.AdminSessionSummary{{Username: "sensitive-user"}},
			}, err: tt.err}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				unusedAccountState{}, unusedAccountList{}, list, unusedAdminSessionRevocation{},
				resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/sessions?limit=2&after="+cursor, nil)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.status || !strings.Contains(recorder.Body.String(), `"code":"`+tt.code+`"`) ||
				!strings.Contains(recorder.Body.String(), `"operationId":"operation-123"`) {
				t.Fatalf("session listing error status=%d", recorder.Code)
			}
			if list.calls != 1 || list.actor.AccountID != principal.AccountID || list.actor.SessionID != principal.SessionID ||
				list.after != cursor || list.limit != 2 {
				t.Fatal("session listing actor was not derived from the authenticated session")
			}
			for _, secret := range []string{"internal-sensitive-value", "opaque-cookie", "sensitive-user"} {
				if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
					t.Fatal("session listing response or logs exposed an internal error or account")
				}
			}
			if tt.status == http.StatusServiceUnavailable && !strings.Contains(logs.String(), "error_type") {
				t.Fatal("session listing service failure was not logged")
			}
		})
	}
}

func TestAdminSessionListingHTTPRejectsInvalidSuccessResults(t *testing.T) {
	id := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))
	secondID := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 16))
	now := time.Now().UTC()
	valid := identity.AdminSessionSummary{
		ID: id, AccountID: "user-1", Username: "user-a", CreatedAt: now,
		IdleExpiresAt: now.Add(time.Minute), AbsoluteExpiresAt: now.Add(time.Hour), Current: true,
	}
	invalidTime := valid
	invalidTime.IdleExpiresAt = now.Add(-time.Second)
	invalidCurrent := valid
	invalidCurrent.Current = false
	invalidUsername := valid
	invalidUsername.Username = "User-A"
	for _, tt := range []struct {
		name   string
		result identity.AdminSessionListResult
	}{
		{"missing operation", identity.AdminSessionListResult{Sessions: []identity.AdminSessionSummary{valid}}},
		{"nil sessions", identity.AdminSessionListResult{OperationID: "operation"}},
		{"invalid ID", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{{ID: "invalid"}}}},
		{"invalid username", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{invalidUsername}}},
		{"invalid expiry", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{invalidTime}}},
		{"incorrect current session", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{invalidCurrent}}},
		{"duplicate ID", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{valid, valid}}},
		{"unsorted IDs", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{
			{ID: secondID, AccountID: "user-2", Username: "user-b", CreatedAt: now, IdleExpiresAt: now.Add(time.Minute), AbsoluteExpiresAt: now.Add(time.Hour)}, valid,
		}}},
		{"invalid next cursor", identity.AdminSessionListResult{OperationID: "operation", Sessions: []identity.AdminSessionSummary{valid}, NextCursor: "wrong"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			list := &capturedAdminSessionList{result: tt.result}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				unusedAccountState{}, unusedAccountList{}, list, unusedAdminSessionRevocation{},
				resetTestSessions{principal: session.Principal{AccountID: "admin", SessionID: id}}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "https://console.example/api/admin/sessions?limit=2", nil)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"session_list_unavailable"`) ||
				!strings.Contains(logs.String(), "invalid result") || strings.Contains(recorder.Body.String(), "user-a") {
				t.Fatalf("invalid session listing was reported as success: status=%d", recorder.Code)
			}
		})
	}
}

func TestSessionAPIRequiresAdminSessionServices(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tt := range []struct {
		list   identity.ListAdminSessions
		revoke identity.RevokeAdminSession
	}{
		{nil, unusedAdminSessionRevocation{}},
		{unusedAdminSessionList{}, nil},
	} {
		_, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
			unusedAccountState{}, unusedAccountList{}, tt.list, tt.revoke, failedSessions{}, "https://console.example", logger)
		if !errors.Is(err, consoleapi.ErrInvalidAPIConfig) {
			t.Fatalf("missing session administration service was accepted: %v", err)
		}
	}
}
