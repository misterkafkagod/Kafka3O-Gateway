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
)

func TestAuthorizedClustersFollowCurrentGrantsAndHideGatewayURLs(t *testing.T) {
	c := newTestConsole(t)
	enableTestClusterAdministration(t, c)
	manager, err := session.NewManager(c.db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	api := c.handler.(*consoleapi.SessionAPI)
	if err := api.EnableGatewayForwarding(c.db, manager); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableGatewayForwarding(c.db, manager); err == nil {
		t.Fatal("forwarding was mounted twice")
	}
	adminID, cookies, csrf := unrestrictedAdmin(t, c)
	ctx := context.Background()
	classify, _ := identity.NewChangeClusterEnvironment(c.db, rand.Reader, time.Now)
	actor := identity.LocalAdminActor{AccountID: adminID, SessionID: sessionIDOf(t, c, cookies)}
	if _, err := classify.Execute(ctx, actor, "prod", "", authorization.EnvironmentProduction, "fixture"); err != nil {
		t.Fatal(err)
	}
	list := func(cookies []*http.Cookie) (int, []map[string]string, string) {
		t.Helper()
		response, data := c.request(t, http.MethodGet, "/api/clusters", "", c.origin, cookies, "")
		var body struct {
			Clusters []map[string]string `json:"clusters"`
		}
		_ = json.Unmarshal(data, &body)
		return response.StatusCode, body.Clusters, string(data)
	}
	status, clusters, raw := list(cookies)
	if status != 200 || len(clusters) != 2 || clusters[1]["id"] != "prod" || clusters[1]["environment"] != "PRODUCTION" ||
		clusters[1]["effectiveRole"] != "ADMIN" || strings.Contains(raw, "gatewayUrl") || strings.Contains(raw, ".example") {
		t.Fatalf("admin clusters = %d %s", status, raw)
	}
	response, data := c.request(t, http.MethodPost, "/api/admin/local-accounts",
		`{"username":"alice","temporaryPassword":"alice temporary passphrase"}`, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create alice = %d", response.StatusCode)
	}
	var created struct {
		Account struct {
			ID string `json:"id"`
		} `json:"account"`
	}
	_ = json.Unmarshal(data, &created)
	_, aliceCookies, aliceCSRF, _ := c.login(t, "alice", "alice temporary passphrase")
	c.request(t, http.MethodPost, "/api/session/password", `{"currentPassword":"alice temporary passphrase","newPassword":"alice replacement passphrase"}`, c.origin, aliceCookies, aliceCSRF)
	_, aliceCookies, _, _ = c.login(t, "alice", "alice replacement passphrase")
	if status, clusters, raw := list(aliceCookies); status != 200 || len(clusters) != 0 {
		t.Fatalf("role-free clusters = %d %s", status, raw)
	}
	grants, _ := identity.NewChangeRoleGrant(c.db, rand.Reader, time.Now)
	if _, err := grants.Execute(ctx, actor, created.Account.ID, "dev", "", authorization.RoleViewer); err != nil {
		t.Fatal(err)
	}
	if status, clusters, raw := list(aliceCookies); status != 200 || len(clusters) != 1 || clusters[0]["id"] != "dev" || clusters[0]["effectiveRole"] != "VIEWER" {
		t.Fatalf("viewer clusters = %d %s", status, raw)
	}
	if status, _, _ := list(nil); status != http.StatusUnauthorized {
		t.Fatalf("anonymous clusters = %d", status)
	}
}

// sessionIDOf returns the current session's opaque ID from the administrator session listing.
func sessionIDOf(t *testing.T, c testConsole, cookies []*http.Cookie) string {
	t.Helper()
	response, data := c.request(t, http.MethodGet, "/api/admin/sessions", "", c.origin, cookies, "")
	var page struct {
		Sessions []struct {
			ID        string `json:"id"`
			IsCurrent bool   `json:"isCurrent"`
		} `json:"sessions"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(data, &page) != nil {
		t.Fatalf("session listing = %d", response.StatusCode)
	}
	for _, entry := range page.Sessions {
		if entry.IsCurrent {
			return entry.ID
		}
	}
	t.Fatal("no current session")
	return ""
}
