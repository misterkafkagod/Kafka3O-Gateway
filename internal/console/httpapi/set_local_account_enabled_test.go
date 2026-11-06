package httpapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func TestAdministratorAccountStateHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	adminID, adminCookies, adminCSRF, restricted := c.login(t, "admin", "initial passphrase")
	if !restricted {
		t.Fatal("bootstrap administrator was not restricted")
	}
	adminPath := "/api/admin/local-accounts/" + adminID + "/enabled"
	response, _ := c.request(t, http.MethodPut, adminPath, `{"enabled":false}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted administrator changed account state: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("administrator password change status=%d", response.StatusCode)
	}
	adminID, adminCookies, adminCSRF, restricted = c.login(t, "admin", "replacement passphrase")
	if restricted {
		t.Fatal("administrator remained restricted")
	}
	adminPath = "/api/admin/local-accounts/" + adminID + "/enabled"
	for _, tt := range []struct {
		name, body, origin, csrf string
		cookies                  []*http.Cookie
		want                     int
	}{
		{"anonymous", `{"enabled":false}`, c.origin, adminCSRF, nil, http.StatusUnauthorized},
		{"wrong origin", `{"enabled":false}`, "https://attacker.example", adminCSRF, adminCookies, http.StatusForbidden},
		{"missing CSRF", `{"enabled":false}`, c.origin, "", adminCookies, http.StatusForbidden},
		{"wrong CSRF", `{"enabled":false}`, c.origin, "incorrect", adminCookies, http.StatusForbidden},
		{"missing enabled", `{}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"null body", `null`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"null enabled", `{"enabled":null}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"string enabled", `{"enabled":"false"}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"duplicate enabled", `{"enabled":true,"enabled":false}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"duplicate case variant", `{"enabled":true,"Enabled":false}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"forged actor", `{"enabled":false,"actorId":"attacker"}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"forged session", `{"enabled":false,"sessionId":"attacker"}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"forged role", `{"enabled":false,"role":"ADMIN"}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"trailing body", `{"enabled":false}{}`, c.origin, adminCSRF, adminCookies, http.StatusBadRequest},
		{"oversized body", `{"enabled":false,"extra":"` + strings.Repeat("x", 17*1024) + `"}`, c.origin, adminCSRF, adminCookies, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, _ := c.request(t, http.MethodPut, adminPath, tt.body, tt.origin, tt.cookies, tt.csrf)
			if response.StatusCode != tt.want {
				t.Fatalf("account state status=%d, want=%d", response.StatusCode, tt.want)
			}
		})
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.disable", Limit: 100})
	if err != nil || len(events) != 0 {
		t.Fatalf("rejected HTTP requests reached the state service: count=%d err=%v", len(events), err)
	}
	response, data := c.request(t, http.MethodPut, adminPath, `{"enabled":false}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusConflict || !bytes.Contains(data, []byte(`"code":"last_local_global_admin"`)) ||
		!bytes.Contains(data, []byte(`"operationId":`)) {
		t.Fatalf("last administrator disable status=%d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPut, "/api/admin/local-accounts/untrusted-target/enabled",
		`{"enabled":false}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusNotFound || bytes.Contains(data, []byte("untrusted-target")) {
		t.Fatalf("unknown account state status=%d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, "/api/admin/local-accounts",
		`{"username":"Example-User","temporaryPassword":"user temporary passphrase"}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("local account creation status=%d", response.StatusCode)
	}
	var created struct {
		Account struct {
			ID string `json:"id"`
		} `json:"account"`
	}
	if err := json.Unmarshal(data, &created); err != nil || created.Account.ID == "" {
		t.Fatal("account creation response omitted the account ID")
	}
	userID, userCookies, userCSRF, restricted := c.login(t, "example-user", "user temporary passphrase")
	if !restricted || userID != created.Account.ID {
		t.Fatal("created user did not have a restricted session")
	}
	userPath := "/api/admin/local-accounts/" + userID + "/enabled"
	response, _ = c.request(t, http.MethodPut, userPath, `{"enabled":false}`, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted user changed account state: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"user temporary passphrase","newPassword":"user replacement passphrase"}`, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("user password change status=%d", response.StatusCode)
	}
	_, userCookies, userCSRF, restricted = c.login(t, "example-user", "user replacement passphrase")
	if restricted {
		t.Fatal("user remained restricted")
	}
	response, data = c.request(t, http.MethodPut, adminPath, `{"enabled":false}`, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("role-free user changed account state: %d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPut, userPath, `{"enabled":false}`, c.origin, adminCookies, adminCSRF)
	var disabled struct {
		OperationID    string `json:"operationId"`
		Enabled        bool   `json:"enabled"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &disabled) != nil ||
		disabled.OperationID == "" || disabled.Enabled || disabled.SignInRequired || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("disable response status=%d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", userCookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled user session remained valid: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/local",
		`{"username":"example-user","password":"user replacement passphrase"}`, c.origin, nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("disabled user could log in: %d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPut, userPath, `{"enabled":true}`, c.origin, adminCookies, adminCSRF)
	var enabled struct {
		OperationID    string `json:"operationId"`
		Enabled        bool   `json:"enabled"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &enabled) != nil ||
		enabled.OperationID == "" || !enabled.Enabled || enabled.SignInRequired || enabled.OperationID == disabled.OperationID {
		t.Fatalf("enable response status=%d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", userCookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("re-enabled user revived an old session: %d", response.StatusCode)
	}
	_, userCookies, _, restricted = c.login(t, "example-user", "user replacement passphrase")
	if restricted {
		t.Fatal("re-enabled user could not log in normally")
	}
	response, _ = c.request(t, http.MethodPut, userPath, `{"enabled":true}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("idempotent enable status=%d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", userCookies, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("idempotent enable revoked a valid session: %d", response.StatusCode)
	}
	all, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var disableRecorded, enableRecorded bool
	for _, event := range all {
		if event.OperationID == disabled.OperationID && event.Action == "console.local_account.disable" &&
			event.Outcome == storage.AuditSucceeded && event.ActorID == adminID && event.ResourceID == userID {
			disableRecorded = true
		}
		if event.OperationID == enabled.OperationID && event.Action == "console.local_account.enable" &&
			event.Outcome == storage.AuditSucceeded && event.ActorID == adminID && event.ResourceID == userID {
			enableRecorded = true
		}
	}
	if !disableRecorded || !enableRecorded {
		t.Fatal("account state changes did not produce correlated audit outcomes")
	}
	serialized := fmt.Sprintf("%+v", all)
	for _, secret := range []string{"untrusted-target", "example-user", "user temporary passphrase", "user replacement passphrase", adminCSRF, userCSRF} {
		if strings.Contains(serialized, secret) {
			t.Fatal("account state audit contained untrusted input or a credential")
		}
	}
}

type capturedAccountState struct {
	actor   identity.LocalAdminActor
	target  string
	enabled bool
	result  identity.LocalAccountStateResult
	err     error
	calls   int
}

func (s *capturedAccountState) Execute(_ context.Context, actor identity.LocalAdminActor, target string, enabled bool) (identity.LocalAccountStateResult, error) {
	s.calls++
	s.actor, s.target, s.enabled = actor, target, enabled
	return s.result, s.err
}

func TestAccountStateHTTPErrorSemanticsAndServerDerivedActor(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
	}{
		{"audit failure with denial", "account_state_audit_unavailable", errors.Join(identity.ErrLastLocalGlobalAdmin, identity.ErrAccountStateAudit), http.StatusServiceUnavailable},
		{"unknown rollback with audit failure", "account_state_outcome_unknown", errors.Join(identity.ErrAccountStateAudit, identity.ErrAccountStateUnknown), http.StatusServiceUnavailable},
		{"unknown commit", "account_state_outcome_unknown", identity.ErrAccountStateUnknown, http.StatusServiceUnavailable},
		{"unexpected failure", "account_state_outcome_unknown", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized},
		{"non-admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden},
		{"password change required", "password_change_required", identity.ErrAdminPasswordChange, http.StatusForbidden},
		{"last administrator", "last_local_global_admin", identity.ErrLastLocalGlobalAdmin, http.StatusConflict},
		{"invalid target", "invalid_account", identity.ErrInvalidLocalAccount, http.StatusBadRequest},
		{"unknown target", "account_not_found", identity.ErrAccountNotFound, http.StatusNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			csrfBytes := bytes.Repeat([]byte{7}, 32)
			csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
			principal := session.Principal{AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes)}
			setEnabled := &capturedAccountState{result: identity.LocalAccountStateResult{OperationID: "operation-123"}, err: tt.err}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				setEnabled, unusedAccountList{}, resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPut, "https://console.example/api/admin/local-accounts/target-id/enabled",
				strings.NewReader(`{"enabled":false}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.status || !strings.Contains(recorder.Body.String(), `"code":"`+tt.code+`"`) ||
				!strings.Contains(recorder.Body.String(), `"operationId":"operation-123"`) {
				t.Fatalf("account state error status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if setEnabled.calls != 1 || setEnabled.actor.AccountID != principal.AccountID || setEnabled.actor.SessionID != principal.SessionID ||
				setEnabled.target != "target-id" || setEnabled.enabled {
				t.Fatal("account state actor was not derived from the authenticated session")
			}
			for _, secret := range []string{"internal-sensitive-value", "opaque-cookie", csrf} {
				if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
					t.Fatal("account state response or logs exposed an internal error or credential")
				}
			}
			if tt.status == http.StatusServiceUnavailable && !strings.Contains(logs.String(), "error_type") {
				t.Fatal("account state service failure was not logged")
			}
		})
	}
}

func TestAccountStateHTTPSelfDisableClearsCookiesOnUnknownOutcome(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result identity.LocalAccountStateResult
		err    error
	}{
		{"unknown commit", identity.LocalAccountStateResult{OperationID: "operation-123"}, identity.ErrAccountStateUnknown},
		{"unexpected failure", identity.LocalAccountStateResult{OperationID: "operation-123"}, errors.New("unexpected")},
		{"missing operation", identity.LocalAccountStateResult{}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			csrfBytes := bytes.Repeat([]byte{7}, 32)
			csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
			principal := session.Principal{AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes)}
			setEnabled := &capturedAccountState{result: tt.result, err: tt.err}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				setEnabled, unusedAccountList{}, resetTestSessions{principal: principal}, "https://console.example", slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPut, "https://console.example/api/admin/local-accounts/server-admin/enabled",
				strings.NewReader(`{"enabled":false}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"account_state_outcome_unknown"`) {
				t.Fatalf("unknown self-disable status=%d", recorder.Code)
			}
			cleared := map[string]bool{}
			for _, cookie := range recorder.Result().Cookies() {
				if cookie.MaxAge < 0 {
					cleared[cookie.Name] = true
				}
			}
			if !cleared[session.CookieName] || !cleared[session.CSRFCookieName] {
				t.Fatal("unknown self-disable did not clear browser cookies")
			}
		})
	}
}

func TestSessionAPIRequiresAccountStateService(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
		nil, unusedAccountList{}, failedSessions{}, "https://console.example", logger)
	if !errors.Is(err, consoleapi.ErrInvalidAPIConfig) {
		t.Fatalf("missing account state service was accepted: %v", err)
	}
}
