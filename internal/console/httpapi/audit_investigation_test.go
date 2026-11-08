package httpapi_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

// testAuditStore mirrors the composition-root adapter for tests.
type testAuditStore struct{ db *storage.Database }

func (s testAuditStore) ListAudit(ctx context.Context, f consoleapi.AuditFilter) ([]consoleapi.AuditRecord, error) {
	events, err := s.db.ListAuditEvents(ctx, storage.AuditFilter{Global: f.Global, ClusterID: f.ClusterID, ActorID: f.ActorID, Action: f.Action,
		Outcome: storage.AuditOutcome(f.Outcome), From: f.From, Until: f.Until, AfterSequence: f.AfterSequence, Limit: f.Limit})
	records := []consoleapi.AuditRecord{}
	for _, e := range events {
		records = append(records, consoleapi.AuditRecord{Sequence: e.Sequence, OperationID: e.OperationID, OccurredAt: e.OccurredAt, ActorID: e.ActorID,
			Source: string(e.Source), ClusterID: e.ClusterID, Action: e.Action, ResourceKind: e.ResourceKind, Decision: e.Decision, Outcome: string(e.Outcome)})
	}
	return records, err
}

func (s testAuditStore) AppendAudit(ctx context.Context, r consoleapi.AuditRecord) error {
	return s.db.AppendAuditEvent(ctx, storage.AuditEvent{EventID: r.EventID, OperationID: r.OperationID, OccurredAt: r.OccurredAt, ActorID: r.ActorID,
		Source: storage.AuditSource(r.Source), ClusterID: r.ClusterID, Action: r.Action, ResourceKind: r.ResourceKind, Decision: r.Decision,
		Outcome: storage.AuditOutcome(r.Outcome)})
}

type auditFixture struct {
	c       testConsole
	admin   []*http.Cookie
	users   map[string][]*http.Cookie
	adminID string
}

func newAuditFixture(t *testing.T) auditFixture {
	t.Helper()
	c := newTestConsole(t)
	enableTestClusterAdministration(t, c)
	manager, _ := session.NewManager(c.db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	api := c.handler.(*consoleapi.SessionAPI)
	if err := api.EnableAuditInvestigation(testAuditStore{c.db}); err == nil {
		t.Fatal("audit mounted before grants")
	}
	if err := api.EnableGatewayForwarding(c.db, manager); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableAuditInvestigation(testAuditStore{c.db}); err != nil {
		t.Fatal(err)
	}
	adminID, cookies, csrf := unrestrictedAdmin(t, c)
	actor := identity.LocalAdminActor{AccountID: adminID, SessionID: sessionIDOf(t, c, cookies)}
	classify, _ := identity.NewChangeClusterEnvironment(c.db, rand.Reader, time.Now)
	if _, err := classify.Execute(context.Background(), actor, "prod", "", authorization.EnvironmentProduction, "fixture"); err != nil {
		t.Fatal(err)
	}
	grants, _ := identity.NewChangeRoleGrant(c.db, rand.Reader, time.Now)
	users := map[string][]*http.Cookie{}
	for _, user := range []struct {
		name, cluster string
		role          authorization.Role
	}{{"alice", "prod", authorization.RoleAdmin}, {"bob", "", authorization.RoleOperator}} {
		response, data := c.request(t, http.MethodPost, "/api/admin/local-accounts",
			`{"username":"`+user.name+`","temporaryPassword":"`+user.name+` temporary passphrase"}`, c.origin, cookies, csrf)
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("create %s = %d", user.name, response.StatusCode)
		}
		var created struct {
			Account struct {
				ID string `json:"id"`
			} `json:"account"`
		}
		_ = json.Unmarshal(data, &created)
		if _, err := grants.Execute(context.Background(), actor, created.Account.ID, user.cluster, "", user.role); err != nil {
			t.Fatal(err)
		}
		_, userCookies, userCSRF, _ := c.login(t, user.name, user.name+" temporary passphrase")
		c.request(t, http.MethodPost, "/api/session/password", `{"currentPassword":"`+user.name+` temporary passphrase","newPassword":"`+user.name+` replacement passphrase"}`,
			c.origin, userCookies, userCSRF)
		_, userCookies, _, _ = c.login(t, user.name, user.name+" replacement passphrase")
		users[user.name] = userCookies
	}
	return auditFixture{c: c, admin: cookies, users: users, adminID: adminID}
}

func (f auditFixture) get(t *testing.T, cookies []*http.Cookie, path string) (int, []map[string]any) {
	t.Helper()
	response, data := f.c.request(t, http.MethodGet, path, "", f.c.origin, cookies, "")
	var body struct {
		Events []map[string]any `json:"events"`
	}
	_ = json.Unmarshal(data, &body)
	return response.StatusCode, body.Events
}

func TestAuditSearchIsScopedAndItselfAudited(t *testing.T) {
	f := newAuditFixture(t)
	status, global := f.get(t, f.admin, "/api/audit?limit=500")
	if status != 200 || len(global) == 0 {
		t.Fatalf("global audit = %d %d events", status, len(global))
	}
	sawGlobalSecurity := false
	for _, event := range global {
		if event["clusterId"] == "" && strings.HasPrefix(event["action"].(string), "console.local_account") {
			sawGlobalSecurity = true
		}
	}
	if !sawGlobalSecurity {
		t.Fatal("global audit omitted global security events")
	}
	status, prod := f.get(t, f.users["alice"], "/api/audit?cluster=prod&limit=500")
	if status != 200 || len(prod) == 0 {
		t.Fatalf("cluster ADMIN prod audit = %d", status)
	}
	for _, event := range prod {
		if event["clusterId"] != "prod" {
			t.Fatalf("cluster audit leaked %+v", event)
		}
	}
	for _, tt := range []struct {
		user, path string
		status     int
	}{
		{"alice", "/api/audit", 403}, {"alice", "/api/audit?cluster=dev", 403}, {"bob", "/api/audit?cluster=prod", 403}, {"bob", "/api/audit", 403},
	} {
		if status, events := f.get(t, f.users[tt.user], tt.path); status != tt.status || len(events) != 0 {
			t.Fatalf("%s %s = %d with %d events", tt.user, tt.path, status, len(events))
		}
	}
	if status, _ := f.get(t, nil, "/api/audit"); status != 401 {
		t.Fatalf("anonymous audit = %d", status)
	}
	for _, path := range []string{"/api/audit?limit=0", "/api/audit?limit=501", "/api/audit?outcome=hacked", "/api/audit?from=yesterday",
		"/api/audit?after=-1", "/api/audit?cluster=a/b", "/api/audit?unknown=1", "/api/audit?actor=a&actor=b",
		"/api/audit?from=2026-11-08T10:00:00Z&until=2026-11-08T09:00:00Z"} {
		if status, _ := f.get(t, f.admin, path); status != 400 {
			t.Fatalf("%s = %d", path, status)
		}
	}
	status, reads := f.get(t, f.admin, "/api/audit?action=console.audit.read&limit=500")
	var aliceRead, aliceDenied bool
	for _, event := range reads {
		if event["clusterId"] == "prod" && event["outcome"] == "succeeded" {
			aliceRead = true
		}
		if event["clusterId"] == "dev" && event["outcome"] == "denied" {
			aliceDenied = true
		}
	}
	if status != 200 || !aliceRead || !aliceDenied {
		t.Fatalf("audit reads were not audited: read=%t denied=%t", aliceRead, aliceDenied)
	}
	status, page := f.get(t, f.admin, "/api/audit?limit=2")
	if status != 200 || len(page) != 2 {
		t.Fatalf("paged audit = %d %d", status, len(page))
	}
}

type unauditableStore struct{ testAuditStore }

func (unauditableStore) AppendAudit(context.Context, consoleapi.AuditRecord) error {
	return context.DeadlineExceeded
}

func TestAuditSearchReturnsNoDataWhenTheReadCannotBeAudited(t *testing.T) {
	c := newTestConsole(t)
	manager, _ := session.NewManager(c.db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	api := c.handler.(*consoleapi.SessionAPI)
	enableTestClusterAdministration(t, c)
	if err := api.EnableGatewayForwarding(c.db, manager); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableAuditInvestigation(unauditableStore{testAuditStore{c.db}}); err != nil {
		t.Fatal(err)
	}
	_, cookies, _ := unrestrictedAdmin(t, c)
	response, data := c.request(t, http.MethodGet, "/api/audit", "", c.origin, cookies, "")
	if response.StatusCode != 503 || strings.Contains(string(data), `"events"`) {
		t.Fatalf("unaudited read = %d %s", response.StatusCode, data)
	}
}

func TestAuditExportIsScopedNeutralizedAndAudited(t *testing.T) {
	f := newAuditFixture(t)
	request := func(cookies []*http.Cookie, path string) (*http.Response, string) {
		t.Helper()
		response, data := f.c.request(t, http.MethodGet, path, "", f.c.origin, cookies, "")
		return response, string(data)
	}
	response, body := request(f.users["alice"], "/api/audit/export?cluster=prod")
	if response.StatusCode != 200 || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/csv") ||
		!strings.Contains(response.Header.Get("Content-Disposition"), "kafka3o-audit-prod-") || !strings.HasPrefix(body, "sequence,occurred_at,") {
		t.Fatalf("cluster export = %d %q", response.StatusCode, body[:min(len(body), 120)])
	}
	for _, line := range strings.Split(strings.TrimSpace(body), "\n")[1:] {
		if !strings.Contains(line, ",prod,") {
			t.Fatalf("cluster export leaked %q", line)
		}
	}
	if response, _ := request(f.users["alice"], "/api/audit/export"); response.StatusCode != 403 {
		t.Fatalf("cluster ADMIN global export = %d", response.StatusCode)
	}
	if response, _ := request(f.users["bob"], "/api/audit/export?cluster=prod"); response.StatusCode != 403 {
		t.Fatalf("OPERATOR export = %d", response.StatusCode)
	}
	if response, body := request(f.admin, "/api/audit/export"); response.StatusCode != 200 || !strings.Contains(body, "console.audit.export") {
		t.Fatalf("global export = %d (the earlier exports should be listed)", response.StatusCode)
	}
}
