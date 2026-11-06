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

func TestPasswordResetHTTPEnforcesLiveAdministratorAndRevokesTarget(t *testing.T) {
	f := newAccountAdministrationFixture(t)
	ctx := context.Background()
	target, targetSession := f.addAccount(t, "subject-1", "other-user", "", "")
	clusterAdmin, clusterSession := f.addAccount(t, "cluster-admin-id", "cluster-admin", "cluster-a", "ADMIN")
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
	reset, err := identity.NewResetLocalPassword(f.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewSessionAPI(login, change, reset, f.manager, "https://console.example",
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	request := func(credentials session.Credentials) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "https://console.example/api/admin/local-accounts/"+target.ID+"/password-reset",
			strings.NewReader(`{"temporaryPassword":"new temporary passphrase"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://console.example")
		req.Header.Set(session.CSRFHeader, credentials.CSRFToken)
		req.AddCookie(&http.Cookie{Name: session.CookieName, Value: credentials.Token})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		return recorder
	}
	denied := request(clusterSession)
	if denied.Code != http.StatusForbidden || !strings.Contains(denied.Body.String(), `"code":"global_admin_required"`) {
		t.Fatalf("cluster-only administrator reset status=%d", denied.Code)
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); err != nil {
		t.Fatalf("denied HTTP reset changed the target session: %v", err)
	}
	allowed := request(f.adminRun)
	if allowed.Code != http.StatusOK {
		t.Fatalf("global administrator reset status=%d", allowed.Code)
	}
	var response struct {
		OperationID    string `json:"operationId"`
		SignInRequired bool   `json:"signInRequired"`
	}
	if err := json.Unmarshal(allowed.Body.Bytes(), &response); err != nil || response.OperationID == "" || response.SignInRequired {
		t.Fatal("reset of another account returned an invalid response")
	}
	if _, err := f.manager.Resolve(ctx, targetSession.Token); !errors.Is(err, session.ErrInvalidSession) {
		t.Fatalf("reset target session remained valid: %v", err)
	}
	if _, err := f.manager.Resolve(ctx, f.adminRun.Token); err != nil {
		t.Fatalf("reset of another account revoked administrator session: %v", err)
	}
	events, err := f.db.ListAuditEvents(ctx, AuditFilter{Global: true, Action: "console.local_password_reset", Limit: 10})
	if err != nil || len(events) != 3 || events[0].ActorID != clusterAdmin.ID || events[0].Outcome != AuditDenied ||
		events[2].ActorID != f.admin.ID || events[2].ResourceID != target.ID || events[2].OperationID != response.OperationID {
		t.Fatalf("HTTP reset audit count=%d err=%v", len(events), err)
	}
}
