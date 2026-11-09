package httpapi_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

type sessionAdministrationBody struct {
	MustChangePassword bool `json:"mustChangePassword"`
	Administration     *struct {
		Access bool `json:"access"`
		Audit  bool `json:"audit"`
	} `json:"administration"`
}

func readSessionAdministration(t *testing.T, c testConsole, cookies []*http.Cookie) (int, sessionAdministrationBody) {
	t.Helper()
	response, data := c.request(t, http.MethodGet, "/api/session", "", "", cookies, "")
	var body sessionAdministrationBody
	if response.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode, body
}

func TestSessionAdministrationFollowsCurrentGrants(t *testing.T) {
	c := newTestConsole(t)
	enableTestClusterAdministration(t, c)
	enableTestAccessAdministration(t, c)
	manager, err := session.NewManager(c.db, rand.Reader, time.Now, session.DefaultIdleLifetime, session.DefaultAbsoluteLifetime)
	if err != nil {
		t.Fatal(err)
	}
	api := c.handler.(*consoleapi.SessionAPI)
	if err := api.EnableGatewayForwarding(c.db, manager); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableAuditInvestigation(testAuditStore{c.db}); err != nil {
		t.Fatal(err)
	}
	adminID, adminCookies, adminCSRF := unrestrictedAdmin(t, c)
	if status, body := readSessionAdministration(t, c, adminCookies); status != http.StatusOK || body.Administration == nil ||
		!body.Administration.Access || !body.Administration.Audit {
		t.Fatalf("global ADMIN administration = %d %+v", status, body.Administration)
	}

	ctx := context.Background()
	actor := identity.LocalAdminActor{AccountID: adminID, SessionID: sessionIDOf(t, c, adminCookies)}
	grants, _ := identity.NewChangeRoleGrant(c.db, rand.Reader, time.Now)
	user := func(name string) (string, []*http.Cookie) {
		t.Helper()
		response, data := c.request(t, http.MethodPost, "/api/admin/local-accounts",
			`{"username":"`+name+`","temporaryPassword":"`+name+` temporary passphrase"}`, c.origin, adminCookies, adminCSRF)
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("create %s = %d", name, response.StatusCode)
		}
		var created struct {
			Account struct {
				ID string `json:"id"`
			} `json:"account"`
		}
		_ = json.Unmarshal(data, &created)
		_, cookies, csrf, _ := c.login(t, name, name+" temporary passphrase")
		if status, body := readSessionAdministration(t, c, cookies); status != http.StatusOK || !body.MustChangePassword ||
			body.Administration == nil || body.Administration.Access || body.Administration.Audit {
			t.Fatalf("%s before the password change = %d %+v", name, status, body.Administration)
		}
		c.request(t, http.MethodPost, "/api/session/password",
			`{"currentPassword":"`+name+` temporary passphrase","newPassword":"`+name+` replacement passphrase"}`, c.origin, cookies, csrf)
		_, cookies, _, _ = c.login(t, name, name+" replacement passphrase")
		return created.Account.ID, cookies
	}

	clusterAdminID, clusterAdmin := user("carol")
	if status, body := readSessionAdministration(t, c, clusterAdmin); status != http.StatusOK || body.Administration.Access || body.Administration.Audit {
		t.Fatalf("role-free administration = %d %+v", status, body.Administration)
	}
	if _, err := grants.Execute(ctx, actor, clusterAdminID, "prod", "", authorization.RoleAdmin); err != nil {
		t.Fatal(err)
	}
	if status, body := readSessionAdministration(t, c, clusterAdmin); status != http.StatusOK || body.Administration.Access || !body.Administration.Audit {
		t.Fatalf("cluster ADMIN administration = %d %+v", status, body.Administration)
	}

	operatorID, operator := user("oscar")
	if _, err := grants.Execute(ctx, actor, operatorID, "", "", authorization.RoleOperator); err != nil {
		t.Fatal(err)
	}
	if status, body := readSessionAdministration(t, c, operator); status != http.StatusOK || body.Administration.Access || body.Administration.Audit {
		t.Fatalf("global OPERATOR administration = %d %+v", status, body.Administration)
	}

	if _, err := grants.Execute(ctx, actor, clusterAdminID, "prod", "ADMIN", authorization.RoleNone); err != nil {
		t.Fatal(err)
	}
	if status, body := readSessionAdministration(t, c, clusterAdmin); status != http.StatusOK || body.Administration.Access || body.Administration.Audit {
		t.Fatalf("revoked cluster ADMIN administration = %d %+v", status, body.Administration)
	}
	if status, _ := readSessionAdministration(t, c, nil); status != http.StatusUnauthorized {
		t.Fatalf("anonymous session = %d", status)
	}
}

func TestSessionAdministrationWithoutGrantsReportsNothing(t *testing.T) {
	c := newTestConsole(t)
	_, cookies, _ := unrestrictedAdmin(t, c)
	if status, body := readSessionAdministration(t, c, cookies); status != http.StatusOK || body.Administration == nil ||
		body.Administration.Access || body.Administration.Audit {
		t.Fatalf("administration without grants = %d %+v", status, body.Administration)
	}
}
