package httpapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

type testConsole struct {
	server  *httptest.Server
	db      *storage.Database
	handler http.Handler
	origin  string
}

func newTestConsole(t *testing.T) testConsole {
	t.Helper()
	ctx := context.Background()
	db, err := storage.OpenDatabase(ctx, filepath.Join(t.TempDir(), "console.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	bootstrap, err := identity.NewBootstrapLocalAdmin(db, secret, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Execute(ctx, secret, "admin", "initial passphrase"); err != nil {
		t.Fatal(err)
	}
	authenticator, err := identity.NewAuthenticateLocal(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	login, err := session.NewLocalLogin(authenticator, manager)
	if err != nil {
		t.Fatal(err)
	}
	change, err := identity.NewChangeLocalPassword(authenticator, db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	reset, err := identity.NewResetLocalPassword(db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	origin := "https://" + server.Listener.Addr().String()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := consoleapi.NewSessionAPI(login, change, reset, manager, origin, logger)
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = handler
	server.StartTLS()
	t.Cleanup(server.Close)
	return testConsole{server: server, db: db, handler: handler, origin: origin}
}

func (c testConsole) request(t *testing.T, method, path, body, origin string, cookies []*http.Cookie, csrf string) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, c.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	if csrf != "" {
		request.Header.Set(session.CSRFHeader, csrf)
	}
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
	response, err := c.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, data
}

func TestLocalSessionHTTPFlow(t *testing.T) {
	c := newTestConsole(t)
	response, _ := c.request(t, http.MethodGet, "/api/session", "", "", nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous session status = %d", response.StatusCode)
	}
	loginBody := `{"username":"admin","password":"initial passphrase"}`
	response, _ = c.request(t, http.MethodPost, "/api/session/local", loginBody, "https://attacker.example", nil, "")
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin login status = %d", response.StatusCode)
	}
	response, wrong := c.request(t, http.MethodPost, "/api/session/local", `{"username":"admin","password":"wrong password"}`, c.origin, nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("incorrect password status = %d", response.StatusCode)
	}
	response, unknown := c.request(t, http.MethodPost, "/api/session/local", `{"username":"unknown","password":"wrong password"}`, c.origin, nil, "")
	if response.StatusCode != http.StatusUnauthorized || !bytes.Equal(wrong, unknown) {
		t.Fatal("unknown user and wrong password have different responses")
	}
	response, data := c.request(t, http.MethodPost, "/api/session/local", loginBody, c.origin, nil, "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", response.StatusCode, data)
	}
	var result struct {
		AccountID          string `json:"accountId"`
		Username           string `json:"username"`
		MustChangePassword bool   `json:"mustChangePassword"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.AccountID == "" || result.Username != "admin" || !result.MustChangePassword {
		t.Fatal("login response did not identify the restricted account")
	}
	var sessionCookie, csrfCookie *http.Cookie
	for _, cookie := range response.Cookies() {
		switch cookie.Name {
		case session.CookieName:
			sessionCookie = cookie
		case session.CSRFCookieName:
			csrfCookie = cookie
		}
	}
	if sessionCookie == nil || csrfCookie == nil || !sessionCookie.HttpOnly || !sessionCookie.Secure ||
		csrfCookie.HttpOnly || !csrfCookie.Secure || sessionCookie.SameSite != http.SameSiteStrictMode ||
		csrfCookie.SameSite != http.SameSiteStrictMode {
		t.Fatal("login did not set the expected secure cookies")
	}
	if bytes.Contains(data, []byte(sessionCookie.Value)) || bytes.Contains(data, []byte(csrfCookie.Value)) {
		t.Fatal("login JSON contained a session credential")
	}
	cookies := []*http.Cookie{sessionCookie, csrfCookie}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", cookies, "")
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("current session status = %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/local", loginBody, c.origin, cookies, "")
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("login without signing out = %d", response.StatusCode)
	}
	changeBody := `{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`
	response, _ = c.request(t, http.MethodPost, "/api/session/password", changeBody, c.origin, cookies, "")
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("password change without CSRF status = %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/password", changeBody, "https://attacker.example", cookies, csrfCookie.Value)
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin password change status = %d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, "/api/session/password", changeBody, c.origin, cookies, csrfCookie.Value)
	if response.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"signInRequired":true`)) {
		t.Fatalf("password change status = %d, body = %s", response.StatusCode, data)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", cookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old session survived password change: %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/local", loginBody, c.origin, nil, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old password survived replacement: %d", response.StatusCode)
	}
	response, data = c.request(t, http.MethodPost, "/api/session/local", `{"username":"admin","password":"replacement passphrase"}`, c.origin, nil, "")
	if response.StatusCode != http.StatusOK || bytes.Contains(data, []byte(`"mustChangePassword":true`)) {
		t.Fatalf("replacement login status = %d", response.StatusCode)
	}
	cookies = response.Cookies()
	for _, cookie := range cookies {
		if cookie.Name == session.CSRFCookieName {
			csrfCookie = cookie
		}
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/logout", "", c.origin, cookies, "wrong")
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("logout without valid CSRF status = %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodPost, "/api/session/logout", "", c.origin, cookies, csrfCookie.Value)
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status = %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", cookies, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("logged-out session still works: %d", response.StatusCode)
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"initial passphrase", "replacement passphrase", "wrong password", sessionCookie.Value, csrfCookie.Value} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatal("audit contained a credential or session token")
		}
	}
}

func TestSessionAPIRejectsInsecureAndMalformedRequests(t *testing.T) {
	c := newTestConsole(t)
	request := httptest.NewRequest(http.MethodGet, c.origin+"/api/session", nil)
	request.TLS = nil
	recorder := httptest.NewRecorder()
	c.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("insecure transport status = %d", recorder.Code)
	}
	request = httptest.NewRequest(http.MethodGet, c.origin+"/api/session", nil)
	request.Host = "other.example"
	recorder = httptest.NewRecorder()
	c.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected Host status = %d", recorder.Code)
	}
	for _, tt := range []struct {
		body string
		want int
	}{
		{`{"username":"admin","password":"initial passphrase","extra":true}`, http.StatusBadRequest},
		{`{"username":"admin"} {}`, http.StatusBadRequest},
		{`{"username":"admin","password":"` + string([]byte{0xff}) + `"}`, http.StatusBadRequest},
		{`{"username":"` + strings.Repeat("a", 17*1024) + `"}`, http.StatusRequestEntityTooLarge},
	} {
		response, _ := c.request(t, http.MethodPost, "/api/session/local", tt.body, c.origin, nil, "")
		if response.StatusCode != tt.want {
			t.Errorf("malformed request status = %d, want %d", response.StatusCode, tt.want)
		}
	}
	response, _ := c.request(t, http.MethodGet, "/api/unmapped", "", "", nil, "")
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("unmapped route status = %d", response.StatusCode)
	}
	response, _ = c.request(t, http.MethodGet, "/api/session", "", "", []*http.Cookie{
		{Name: session.CookieName, Value: "one"}, {Name: session.CookieName, Value: "two"},
	}, "")
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("duplicate session cookies status = %d", response.StatusCode)
	}
}

type failedSessions struct{}

func (failedSessions) Resolve(context.Context, string) (session.Principal, error) {
	return session.Principal{}, errors.New("secret value must not be logged")
}

func (failedSessions) Revoke(context.Context, string) error { return errors.New("unexpected") }

type unusedLogin struct{}

func (unusedLogin) Execute(context.Context, string, string, netip.Addr) (session.Credentials, error) {
	return session.Credentials{}, errors.New("unexpected")
}

type unusedPasswordChange struct{}

func (unusedPasswordChange) Execute(context.Context, string, string, string, string, netip.Addr) error {
	return errors.New("unexpected")
}

type unusedPasswordReset struct{}

func (unusedPasswordReset) Execute(context.Context, identity.LocalAdminActor, string, string) (identity.PasswordResetResult, error) {
	return identity.PasswordResetResult{}, errors.New("unexpected")
}

func TestSessionAPIReportsFailuresWithoutLoggingSecrets(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedPasswordReset{}, failedSessions{}, "https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://console.example/api/session", nil)
	request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("session storage outage status = %d", recorder.Code)
	}
	if strings.Contains(logs.String(), "secret value") || strings.Contains(recorder.Body.String(), "secret value") ||
		strings.Contains(logs.String(), "opaque-cookie") || strings.Contains(recorder.Body.String(), "opaque-cookie") {
		t.Fatal("an internal error or session token was exposed")
	}
	if !strings.Contains(logs.String(), "error_type") {
		t.Fatal("session storage outage was not logged")
	}
}

type throttledLogin struct {
	client netip.Addr
}

func (l *throttledLogin) Execute(_ context.Context, _, _ string, client netip.Addr) (session.Credentials, error) {
	l.client = client
	return session.Credentials{}, identity.ErrAuthenticationThrottled
}

func TestLoginThrottleUsesDirectPeerRatherThanForwardedHeaders(t *testing.T) {
	login := &throttledLogin{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := consoleapi.NewSessionAPI(login, unusedPasswordChange{}, unusedPasswordReset{}, failedSessions{}, "https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "https://console.example/api/session/local",
		strings.NewReader(`{"username":"admin","password":"irrelevant"}`))
	request.RemoteAddr = "192.0.2.1:12345"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "https://console.example")
	request.Header.Set("X-Forwarded-For", "203.0.113.8")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusTooManyRequests || recorder.Header().Get("Retry-After") != "900" {
		t.Fatalf("throttled login response = %d, Retry-After = %q", recorder.Code, recorder.Header().Get("Retry-After"))
	}
	if login.client != netip.MustParseAddr("192.0.2.1") {
		t.Fatal("login trusted an unverified forwarded address")
	}
}
