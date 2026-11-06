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

func TestAdministratorPasswordResetHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	login := func(password string) (string, []*http.Cookie, string, bool) {
		t.Helper()
		body, err := json.Marshal(struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}{Username: "admin", Password: password})
		if err != nil {
			t.Fatal(err)
		}
		response, data := c.request(t, http.MethodPost, "/api/session/local", string(body), c.origin, nil, "")
		if response.StatusCode != http.StatusOK {
			t.Fatalf("administrator login status=%d", response.StatusCode)
		}
		var value struct {
			AccountID          string `json:"accountId"`
			MustChangePassword bool   `json:"mustChangePassword"`
		}
		if err := json.Unmarshal(data, &value); err != nil || value.AccountID == "" {
			t.Fatal("administrator login response was invalid")
		}
		var csrf string
		for _, cookie := range response.Cookies() {
			if cookie.Name == session.CSRFCookieName {
				csrf = cookie.Value
			}
		}
		if csrf == "" {
			t.Fatal("administrator login did not provide a CSRF cookie")
		}
		return value.AccountID, response.Cookies(), csrf, value.MustChangePassword
	}

	adminID, cookies, csrf, restricted := login("initial passphrase")
	if !restricted {
		t.Fatal("bootstrap credential was not restricted")
	}
	path := "/api/admin/local-accounts/" + adminID + "/password-reset"
	body := `{"temporaryPassword":"new temporary passphrase"}`
	response, _ := c.request(t, http.MethodPost, path, body, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("temporary administrator credential could reset passwords: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("administrator password change status=%d", response.StatusCode)
	}
	adminID, cookies, csrf, restricted = login("replacement passphrase")
	if restricted {
		t.Fatal("changed administrator credential remained restricted")
	}
	path = "/api/admin/local-accounts/" + adminID + "/password-reset"
	for _, tt := range []struct {
		name, body, origin, csrf string
		want                     int
	}{
		{"wrong origin", body, "https://attacker.example", csrf, http.StatusForbidden},
		{"missing CSRF", body, c.origin, "", http.StatusForbidden},
		{"wrong CSRF", body, c.origin, "incorrect", http.StatusForbidden},
		{"forged actor", `{"temporaryPassword":"new temporary passphrase","actorId":"attacker"}`, c.origin, csrf, http.StatusBadRequest},
		{"forged session", `{"temporaryPassword":"new temporary passphrase","sessionId":"attacker"}`, c.origin, csrf, http.StatusBadRequest},
		{"oversized body", `{"temporaryPassword":"` + strings.Repeat("x", 17*1024) + `"}`, c.origin, csrf, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, _ := c.request(t, http.MethodPost, path, tt.body, tt.origin, cookies, tt.csrf)
			if response.StatusCode != tt.want {
				t.Fatalf("reset status=%d, want=%d", response.StatusCode, tt.want)
			}
		})
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 100})
	if err != nil || len(events) != 0 {
		t.Fatalf("rejected HTTP requests reached the reset service: count=%d err=%v", len(events), err)
	}
	response, data := c.request(t, http.MethodPost, path, `{"temporaryPassword":"short"}`, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusBadRequest || !bytes.Contains(data, []byte(`"code":"invalid_password"`)) ||
		!bytes.Contains(data, []byte(`"operationId":`)) {
		t.Fatalf("invalid password response status=%d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, "/api/admin/local-accounts/unknown-sensitive-target/password-reset",
		body, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusNotFound || bytes.Contains(data, []byte("unknown-sensitive-target")) {
		t.Fatalf("unknown account response status=%d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, path, body, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("self-reset status=%d", response.StatusCode)
	}
	var result struct {
		OperationID    string `json:"operationId"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.OperationID == "" || !result.SignInRequired {
		t.Fatal("self-reset did not report a correlated sign-out")
	}
	cleared := map[string]bool{}
	for _, cookie := range response.Cookies() {
		if cookie.MaxAge < 0 {
			cleared[cookie.Name] = true
		}
	}
	if !cleared[session.CookieName] || !cleared[session.CSRFCookieName] {
		t.Fatal("self-reset did not clear both browser cookies")
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", cookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("self-reset session remained valid: %d", response.StatusCode)
	}
	_, newCookies, newCSRF, restricted := login("new temporary passphrase")
	if !restricted {
		t.Fatal("reset password did not require a change")
	}
	response, _ = c.request(t, http.MethodPost, path, body, c.origin, newCookies, newCSRF)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("reset session could perform administrative actions: %d", response.StatusCode)
	}
	events, err = c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	serialized := fmt.Sprintf("%+v", events)
	for _, secret := range []string{"initial passphrase", "replacement passphrase", "new temporary passphrase",
		"unknown-sensitive-target", csrf, newCSRF} {
		if strings.Contains(serialized, secret) || bytes.Contains(data, []byte(secret)) {
			t.Fatal("reset response or audit contained a credential or untrusted target")
		}
	}
}

type resetTestSessions struct {
	principal session.Principal
}

func (s resetTestSessions) Resolve(context.Context, string) (session.Principal, error) {
	return s.principal, nil
}

func (resetTestSessions) Revoke(context.Context, string) error { return errors.New("unexpected") }

type capturedReset struct {
	actor    identity.LocalAdminActor
	target   string
	password string
	err      error
	calls    int
	emptyID  bool
}

func (s *capturedReset) Execute(_ context.Context, actor identity.LocalAdminActor, target, password string) (identity.PasswordResetResult, error) {
	s.calls++
	s.actor, s.target, s.password = actor, target, password
	if s.emptyID {
		return identity.PasswordResetResult{}, s.err
	}
	return identity.PasswordResetResult{OperationID: "operation-123"}, s.err
}

func TestPasswordResetHTTPErrorSemanticsAndServerDerivedActor(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
	}{
		{"audit failure with invalid password", "password_reset_audit_unavailable", errors.Join(identity.ErrInvalidPassword, identity.ErrPasswordResetAudit), http.StatusServiceUnavailable},
		{"unknown commit", "password_reset_outcome_unknown", identity.ErrPasswordResetUnknown, http.StatusServiceUnavailable},
		{"unexpected failure", "password_reset_outcome_unknown", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized},
		{"non-admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden},
		{"password change required", "password_change_required", identity.ErrAdminPasswordChange, http.StatusForbidden},
		{"throttled", "password_reset_throttled", identity.ErrPasswordResetThrottled, http.StatusTooManyRequests},
		{"invalid password", "invalid_password", identity.ErrInvalidPassword, http.StatusBadRequest},
		{"invalid target", "invalid_account", identity.ErrInvalidLocalAccount, http.StatusBadRequest},
		{"unknown target", "account_not_found", identity.ErrAccountNotFound, http.StatusNotFound},
		{"changed target", "account_changed", identity.ErrLocalAccountChanged, http.StatusConflict},
		{"expired", "password_reset_expired", identity.ErrPasswordResetExpired, http.StatusConflict},
		{"expired validation", "password_reset_expired", errors.Join(identity.ErrInvalidPassword, identity.ErrPasswordResetExpired), http.StatusConflict},
		{"used", "password_reset_expired", identity.ErrInvalidPasswordReset, http.StatusConflict},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			csrfBytes := bytes.Repeat([]byte{7}, 32)
			csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
			principal := session.Principal{
				AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes),
			}
			reset := &capturedReset{err: tt.err}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, reset,
				resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/local-accounts/target-id/password-reset",
				strings.NewReader(`{"temporaryPassword":"synthetic temporary password"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.status || !strings.Contains(recorder.Body.String(), `"code":"`+tt.code+`"`) ||
				!strings.Contains(recorder.Body.String(), `"operationId":"operation-123"`) {
				t.Fatalf("reset error status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if reset.calls != 1 || reset.actor.AccountID != principal.AccountID || reset.actor.SessionID != principal.SessionID ||
				reset.target != "target-id" || reset.password != "synthetic temporary password" {
				t.Fatal("reset actor was not derived from the authenticated session")
			}
			if tt.status == http.StatusTooManyRequests && recorder.Header().Get("Retry-After") != "900" {
				t.Fatal("reset throttle did not provide Retry-After")
			}
			for _, secret := range []string{"internal-sensitive-value", "synthetic temporary password", "opaque-cookie", csrf} {
				if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
					t.Fatal("reset response or logs exposed an internal error or credential")
				}
			}
			if tt.status == http.StatusServiceUnavailable && !strings.Contains(logs.String(), "error_type") {
				t.Fatal("reset service failure was not logged")
			}
		})
	}
}

func TestPasswordResetHTTPRejectsSuccessWithoutOperationID(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	csrfBytes := bytes.Repeat([]byte{7}, 32)
	csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
	principal := session.Principal{AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes)}
	reset := &capturedReset{emptyID: true}
	handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, reset,
		resetTestSessions{principal: principal}, "https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/local-accounts/target-id/password-reset",
		strings.NewReader(`{"temporaryPassword":"synthetic temporary password"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "https://console.example")
	request.Header.Set(session.CSRFHeader, csrf)
	request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"password_reset_outcome_unknown"`) ||
		!strings.Contains(logs.String(), "no operation ID") || reset.calls != 1 {
		t.Fatalf("reset without an operation ID was reported as success: status=%d", recorder.Code)
	}
}

func TestSessionAPIRequiresPasswordResetService(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, nil, failedSessions{}, "https://console.example", logger)
	if !errors.Is(err, consoleapi.ErrInvalidAPIConfig) {
		t.Fatalf("missing reset service was accepted: %v", err)
	}
}
