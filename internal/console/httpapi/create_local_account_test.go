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

func TestAdministratorLocalAccountCreationHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	_, adminCookies, adminCSRF, restricted := c.login(t, "admin", "initial passphrase")
	if !restricted {
		t.Fatal("bootstrap administrator was not restricted")
	}
	const path = "/api/admin/local-accounts"
	const body = `{"username":"Example-User","temporaryPassword":"new temporary passphrase"}`
	response, _ := c.request(t, http.MethodPost, path, body, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted administrator created an account: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("administrator password change status=%d", response.StatusCode)
	}
	_, adminCookies, adminCSRF, restricted = c.login(t, "admin", "replacement passphrase")
	if restricted {
		t.Fatal("administrator remained restricted after changing password")
	}
	for _, tt := range []struct {
		name, body, origin, csrf string
		want                     int
	}{
		{"wrong origin", body, "https://attacker.example", adminCSRF, http.StatusForbidden},
		{"missing CSRF", body, c.origin, "", http.StatusForbidden},
		{"wrong CSRF", body, c.origin, "incorrect", http.StatusForbidden},
		{"forged actor", `{"username":"Example-User","temporaryPassword":"new temporary passphrase","actorId":"attacker"}`, c.origin, adminCSRF, http.StatusBadRequest},
		{"forged session", `{"username":"Example-User","temporaryPassword":"new temporary passphrase","sessionId":"attacker"}`, c.origin, adminCSRF, http.StatusBadRequest},
		{"implicit role", `{"username":"Example-User","temporaryPassword":"new temporary passphrase","role":"ADMIN"}`, c.origin, adminCSRF, http.StatusBadRequest},
		{"oversized body", `{"username":"user","temporaryPassword":"` + strings.Repeat("x", 17*1024) + `"}`, c.origin, adminCSRF, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, _ := c.request(t, http.MethodPost, path, tt.body, tt.origin, adminCookies, tt.csrf)
			if response.StatusCode != tt.want {
				t.Fatalf("account creation status=%d, want=%d", response.StatusCode, tt.want)
			}
		})
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Action: "console.local_account.create", Limit: 100})
	if err != nil || len(events) != 0 {
		t.Fatalf("rejected HTTP requests reached the creation service: count=%d err=%v", len(events), err)
	}
	response, data := c.request(t, http.MethodPost, path, body, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusCreated || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("local account creation status=%d", response.StatusCode)
	}
	var result struct {
		OperationID string `json:"operationId"`
		Account     struct {
			ID                 string `json:"id"`
			Username           string `json:"username"`
			Enabled            bool   `json:"enabled"`
			MustChangePassword bool   `json:"mustChangePassword"`
		} `json:"account"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.OperationID == "" || result.Account.ID == "" ||
		result.Account.Username != "example-user" || !result.Account.Enabled || !result.Account.MustChangePassword {
		t.Fatal("account creation response was invalid")
	}
	for _, forbidden := range []string{"new temporary passphrase", "passwordHash", "credentialVersion", "role"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatal("account creation response contained a credential or grant")
		}
	}
	grants, err := c.db.ListAccountGrants(context.Background(), result.Account.ID)
	if err != nil || len(grants) != 0 {
		t.Fatalf("new account received implicit grants: count=%d err=%v", len(grants), err)
	}
	response, data = c.request(t, http.MethodPost, path, body, c.origin, adminCookies, adminCSRF)
	if response.StatusCode != http.StatusConflict || !bytes.Contains(data, []byte(`"code":"username_in_use"`)) {
		t.Fatalf("duplicate username status=%d", response.StatusCode)
	}
	userID, userCookies, userCSRF, restricted := c.login(t, "example-user", "new temporary passphrase")
	if userID != result.Account.ID || !restricted {
		t.Fatal("new account did not require a password change")
	}
	response, _ = c.request(t, http.MethodPost, path, body, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("temporary user could create accounts: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"new temporary passphrase","newPassword":"user replacement passphrase"}`, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("new user password change status=%d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", userCookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("new user's temporary session survived password change: %d", response.StatusCode)
	}
	_, userCookies, userCSRF, restricted = c.login(t, "example-user", "user replacement passphrase")
	if restricted {
		t.Fatal("new user remained restricted after password change")
	}
	response, data = c.request(t, http.MethodPost, path, body, c.origin, userCookies, userCSRF)
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("role-free user could create accounts: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", adminCookies, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("creating another account invalidated the administrator session: %d", response.StatusCode)
	}
	all, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	serialized := fmt.Sprintf("%+v", all)
	for _, secret := range []string{"Example-User", "example-user", "new temporary passphrase", "user replacement passphrase", adminCSRF, userCSRF} {
		if strings.Contains(serialized, secret) {
			t.Fatal("account creation audit contained a username or credential")
		}
	}
}

type capturedCreation struct {
	actor    identity.LocalAdminActor
	username string
	password string
	result   identity.LocalAccountCreationResult
	err      error
	calls    int
}

func (s *capturedCreation) Execute(_ context.Context, actor identity.LocalAdminActor, username, password string) (identity.LocalAccountCreationResult, error) {
	s.calls++
	s.actor, s.username, s.password = actor, username, password
	return s.result, s.err
}

func TestLocalAccountCreationHTTPErrorSemanticsAndServerDerivedActor(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
	}{
		{"audit failure with invalid username", "account_creation_audit_unavailable", errors.Join(identity.ErrInvalidUsername, identity.ErrAccountCreationAudit), http.StatusServiceUnavailable},
		{"unknown commit", "account_creation_outcome_unknown", identity.ErrAccountCreationUnknown, http.StatusServiceUnavailable},
		{"unexpected failure", "account_creation_outcome_unknown", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized},
		{"non-admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden},
		{"password change required", "password_change_required", identity.ErrAdminPasswordChange, http.StatusForbidden},
		{"invalid username", "invalid_username", identity.ErrInvalidUsername, http.StatusBadRequest},
		{"invalid password", "invalid_password", identity.ErrInvalidPassword, http.StatusBadRequest},
		{"duplicate username", "username_in_use", identity.ErrUsernameTaken, http.StatusConflict},
		{"expired", "account_creation_expired", identity.ErrAccountCreationExpired, http.StatusConflict},
		{"expired validation", "account_creation_expired", errors.Join(identity.ErrInvalidUsername, identity.ErrAccountCreationExpired), http.StatusConflict},
		{"used", "account_creation_expired", identity.ErrInvalidAccountCreation, http.StatusConflict},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			csrfBytes := bytes.Repeat([]byte{7}, 32)
			csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
			principal := session.Principal{AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes)}
			create := &capturedCreation{result: identity.LocalAccountCreationResult{OperationID: "operation-123"}, err: tt.err}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, create, unusedPasswordReset{}, unusedAccountState{}, unusedAccountList{},
				resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/local-accounts",
				strings.NewReader(`{"username":"new-user","temporaryPassword":"synthetic temporary password"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.status || !strings.Contains(recorder.Body.String(), `"code":"`+tt.code+`"`) ||
				!strings.Contains(recorder.Body.String(), `"operationId":"operation-123"`) {
				t.Fatalf("creation error status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if create.calls != 1 || create.actor.AccountID != principal.AccountID || create.actor.SessionID != principal.SessionID ||
				create.username != "new-user" || create.password != "synthetic temporary password" {
				t.Fatal("creation actor was not derived from the authenticated session")
			}
			for _, secret := range []string{"internal-sensitive-value", "new-user", "synthetic temporary password", "opaque-cookie", csrf} {
				if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
					t.Fatal("creation response or logs exposed an internal error or credential")
				}
			}
			if tt.status == http.StatusServiceUnavailable && !strings.Contains(logs.String(), "error_type") {
				t.Fatal("account creation service failure was not logged")
			}
		})
	}
}

func TestLocalAccountCreationHTTPRejectsInvalidSuccessResults(t *testing.T) {
	valid := identity.LocalAccount{ID: "account-id", Username: "new-user", Enabled: true, MustChangePassword: true, CredentialVersion: 1}
	for _, tt := range []struct {
		name   string
		result identity.LocalAccountCreationResult
	}{
		{"missing operation", identity.LocalAccountCreationResult{Account: valid}},
		{"missing account", identity.LocalAccountCreationResult{OperationID: "operation-123"}},
		{"unrestricted account", identity.LocalAccountCreationResult{OperationID: "operation-123", Account: identity.LocalAccount{ID: "account-id", Username: "new-user", Enabled: true, CredentialVersion: 1}}},
		{"password hash", identity.LocalAccountCreationResult{OperationID: "operation-123", Account: identity.LocalAccount{ID: "account-id", Username: "new-user", PasswordHash: "internal-sensitive-hash", Enabled: true, MustChangePassword: true, CredentialVersion: 1}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			csrfBytes := bytes.Repeat([]byte{7}, 32)
			csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
			principal := session.Principal{AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes)}
			create := &capturedCreation{result: tt.result}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, create, unusedPasswordReset{}, unusedAccountState{}, unusedAccountList{},
				resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/local-accounts",
				strings.NewReader(`{"username":"new-user","temporaryPassword":"synthetic temporary password"}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"account_creation_outcome_unknown"`) ||
				!strings.Contains(logs.String(), "invalid result") || create.calls != 1 ||
				strings.Contains(recorder.Body.String(), "internal-sensitive-hash") || strings.Contains(logs.String(), "internal-sensitive-hash") {
				t.Fatalf("invalid creation result was reported as success: status=%d", recorder.Code)
			}
		})
	}
}

func TestSessionAPIRequiresAccountCreationService(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, nil, unusedPasswordReset{}, unusedAccountState{}, unusedAccountList{}, failedSessions{}, "https://console.example", logger)
	if !errors.Is(err, consoleapi.ErrInvalidAPIConfig) {
		t.Fatalf("missing account creation service was accepted: %v", err)
	}
}
