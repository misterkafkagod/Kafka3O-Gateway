package httpapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type capturedAdminSessionRevocation struct {
	actor   identity.LocalAdminActor
	target  string
	err     error
	calls   int
	emptyID bool
}

func (s *capturedAdminSessionRevocation) Execute(_ context.Context, actor identity.LocalAdminActor, target string) (identity.AdminSessionRevocationResult, error) {
	s.calls++
	s.actor, s.target = actor, target
	if s.emptyID {
		return identity.AdminSessionRevocationResult{}, s.err
	}
	return identity.AdminSessionRevocationResult{OperationID: "operation-123"}, s.err
}

func TestAdminSessionRevocationHTTPErrorSemanticsAndServerDerivedActor(t *testing.T) {
	otherID := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))
	selfID := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 16))
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
		self       bool
		clear      bool
		emptyID    bool
	}{
		{"unknown takes precedence over audit", "session_revoke_outcome_unknown", errors.Join(identity.ErrAdminSessionRevokeUnknown, identity.ErrAdminSessionRevokeAudit), http.StatusServiceUnavailable, true, true, false},
		{"audit failure with denial", "session_revoke_audit_unavailable", errors.Join(identity.ErrGlobalAdminRequired, identity.ErrAdminSessionRevokeAudit), http.StatusServiceUnavailable, true, false, false},
		{"unexpected self failure", "session_revoke_outcome_unknown", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable, true, true, false},
		{"unexpected other failure", "session_revoke_outcome_unknown", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable, false, false, false},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized, false, true, false},
		{"non-admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden, false, false, false},
		{"password change required", "password_change_required", identity.ErrAdminPasswordChange, http.StatusForbidden, false, false, false},
		{"invalid target", "invalid_session", identity.ErrInvalidSessionID, http.StatusBadRequest, false, false, false},
		{"unavailable target", "session_unavailable", identity.ErrAdminSessionUnavailable, http.StatusNotFound, false, false, false},
		{"self success without operation", "session_revoke_outcome_unknown", nil, http.StatusServiceUnavailable, true, true, true},
		{"other success without operation", "session_revoke_outcome_unknown", nil, http.StatusServiceUnavailable, false, false, true},
		{"self success", "", nil, http.StatusOK, true, true, false},
		{"other success", "", nil, http.StatusOK, false, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			csrfBytes := bytes.Repeat([]byte{7}, 32)
			csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
			principal := session.Principal{AccountID: "server-admin", SessionID: selfID, CSRFHash: sha256.Sum256(csrfBytes)}
			target := otherID
			if tt.self {
				target = selfID
			}
			revocation := &capturedAdminSessionRevocation{err: tt.err, emptyID: tt.emptyID}
			handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
				unusedAccountState{}, unusedAccountList{}, unusedAdminSessionList{}, revocation,
				resetTestSessions{principal: principal}, "https://console.example", logger)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/sessions/"+target+"/revoke", nil)
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.status || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("session revocation status=%d want=%d", recorder.Code, tt.status)
			}
			if tt.code != "" && !strings.Contains(recorder.Body.String(), `"code":"`+tt.code+`"`) {
				t.Fatal("session revocation returned the wrong error")
			}
			if !tt.emptyID && !strings.Contains(recorder.Body.String(), `"operationId":"operation-123"`) {
				t.Fatal("session revocation omitted its operation ID")
			}
			if tt.status == http.StatusOK && !strings.Contains(recorder.Body.String(), `"signInRequired":`+map[bool]string{true: "true", false: "false"}[tt.self]) {
				t.Fatal("session revocation returned an incorrect self-revocation result")
			}
			if revocation.calls != 1 || revocation.actor.AccountID != principal.AccountID ||
				revocation.actor.SessionID != principal.SessionID || revocation.target != target {
				t.Fatal("session revocation did not use the authenticated actor and path target")
			}
			cleared := map[string]bool{}
			for _, cookie := range recorder.Result().Cookies() {
				if cookie.MaxAge < 0 {
					cleared[cookie.Name] = true
				}
			}
			if cleared[session.CookieName] != tt.clear || cleared[session.CSRFCookieName] != tt.clear {
				t.Fatal("session revocation did not clear cookies appropriately")
			}
			for _, secret := range []string{"internal-sensitive-value", "opaque-cookie", csrf, target} {
				if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
					t.Fatal("session revocation response or logs exposed an internal error or credential")
				}
			}
			if tt.status == http.StatusServiceUnavailable && logs.Len() == 0 {
				t.Fatal("session revocation service failure was not logged")
			}
		})
	}
}

type failingSessionRequestBody struct{}

func (failingSessionRequestBody) Read([]byte) (int, error) {
	return 0, errors.New("sensitive body read failure")
}

func TestAdminSessionRevocationHTTPRejectsUnreadableBodyAndDuplicateCSRF(t *testing.T) {
	csrfBytes := bytes.Repeat([]byte{7}, 32)
	csrf := base64.RawURLEncoding.EncodeToString(csrfBytes)
	principal := session.Principal{AccountID: "server-admin", SessionID: "server-session", CSRFHash: sha256.Sum256(csrfBytes)}
	revocation := &capturedAdminSessionRevocation{}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	handler, err := consoleapi.NewSessionAPI(unusedLogin{}, unusedPasswordChange{}, unusedAccountCreation{}, unusedPasswordReset{},
		unusedAccountState{}, unusedAccountList{}, unusedAdminSessionList{}, revocation,
		resetTestSessions{principal: principal}, "https://console.example", logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name          string
		body          io.Reader
		duplicateCSRF bool
		want          int
	}{
		{"unreadable body", failingSessionRequestBody{}, false, http.StatusBadRequest},
		{"whitespace body", strings.NewReader(" "), false, http.StatusBadRequest},
		{"duplicate CSRF", nil, true, http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/sessions/target/revoke", tt.body)
			request.Header.Set("Origin", "https://console.example")
			request.Header.Set(session.CSRFHeader, csrf)
			if tt.duplicateCSRF {
				request.Header.Add(session.CSRFHeader, csrf)
			}
			request.AddCookie(&http.Cookie{Name: session.CookieName, Value: "opaque-cookie"})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.want || revocation.calls != 0 ||
				strings.Contains(logs.String(), "sensitive body read failure") ||
				strings.Contains(recorder.Body.String(), "sensitive body read failure") {
				t.Fatalf("invalid session revocation request status=%d", recorder.Code)
			}
		})
	}
}
