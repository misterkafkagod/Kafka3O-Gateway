package httpapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/authorization"
	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func enableTestAccessAdministration(t *testing.T, c testConsole) {
	t.Helper()
	list, err := identity.NewListAccessBindings(c.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	grants, err := identity.NewChangeRoleGrant(c.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	mappings, err := identity.NewChangeGroupMapping(c.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	api := c.handler.(*consoleapi.SessionAPI)
	if err := api.EnableAccessAdministration(list, grants, mappings); err != nil {
		t.Fatal(err)
	}
	if err := api.EnableAccessAdministration(list, grants, mappings); !errors.Is(err, consoleapi.ErrInvalidAPIConfig) {
		t.Fatal("access administration was mounted twice")
	}
}

type accessListing struct {
	PolicyRevision int64 `json:"policyRevision"`
	Identities     []struct {
		ID     string `json:"id"`
		Source string `json:"source"`
		Label  string `json:"label"`
		Grants []struct {
			ClusterID string `json:"clusterId"`
			Role      string `json:"role"`
		} `json:"grants"`
	} `json:"identities"`
	GroupGrants []struct {
		Issuer, Group, ClusterID, Role string
	} `json:"groupGrants"`
}

// unrestrictedAdmin replaces the bootstrap administrator's temporary password and signs in again.
func unrestrictedAdmin(t *testing.T, c testConsole) (string, []*http.Cookie, string) {
	t.Helper()
	_, cookies, csrf, _ := c.login(t, "admin", "initial passphrase")
	response, _ := c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"initial passphrase","newPassword":"replacement passphrase"}`, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("password change status=%d", response.StatusCode)
	}
	adminID, cookies, csrf, restricted := c.login(t, "admin", "replacement passphrase")
	if restricted {
		t.Fatal("administrator remained restricted")
	}
	return adminID, cookies, csrf
}

func TestAccessAdministrationHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	enableTestAccessAdministration(t, c)
	adminID, cookies, csrf := unrestrictedAdmin(t, c)
	response, data := c.request(t, http.MethodPost, "/api/admin/local-accounts",
		`{"username":"alice","temporaryPassword":"alice temporary passphrase"}`, c.origin, cookies, csrf)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create alice status=%d", response.StatusCode)
	}
	var created struct {
		Account struct {
			ID string `json:"id"`
		} `json:"account"`
	}
	if err := json.Unmarshal(data, &created); err != nil {
		t.Fatal(err)
	}
	aliceID := created.Account.ID

	grant := func(cookies []*http.Cookie, csrf, target, cluster, expected, role string) (*http.Response, []byte) {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"identityId": target, "clusterId": cluster, "expectedRole": expected, "role": role})
		return c.request(t, http.MethodPut, "/api/admin/role-grants", string(body), c.origin, cookies, csrf)
	}
	for _, tt := range []struct{ cluster, role string }{{"", "VIEWER"}, {"cluster-a", "ADMIN"}} {
		if response, data := grant(cookies, csrf, aliceID, tt.cluster, "", tt.role); response.StatusCode != http.StatusOK ||
			!bytes.Contains(data, []byte(`"operationId":`)) || !bytes.Contains(data, []byte(`"policyRevision":`)) {
			t.Fatalf("grant %+v status=%d body=%s", tt, response.StatusCode, data)
		}
	}
	mapping := `{"issuer":"https://issuer.example","group":"Kafka Ops","clusterId":"cluster-b","expectedRole":"","role":"OPERATOR"}`
	if response, data := c.request(t, http.MethodPut, "/api/admin/group-mappings", mapping, c.origin, cookies, csrf); response.StatusCode != http.StatusOK {
		t.Fatalf("group mapping status=%d body=%s", response.StatusCode, data)
	}
	response, data = c.request(t, http.MethodGet, "/api/admin/access?limit=10", "", c.origin, cookies, "")
	var listing accessListing
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" || json.Unmarshal(data, &listing) != nil {
		t.Fatalf("access listing status=%d body=%s", response.StatusCode, data)
	}
	if listing.PolicyRevision != 3 || len(listing.Identities) != 2 || len(listing.GroupGrants) != 1 ||
		listing.GroupGrants[0].Group != "Kafka Ops" || listing.GroupGrants[0].Role != "OPERATOR" {
		t.Fatalf("access listing = %+v", listing)
	}
	for _, entry := range listing.Identities {
		if entry.ID == aliceID && (entry.Source != "local" || entry.Label != "alice" || len(entry.Grants) != 2) {
			t.Fatalf("alice listing = %+v", entry)
		}
	}
	for _, secret := range []string{"passphrase", "password", "hash", csrf} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("access listing exposed %q", secret)
		}
	}

	if response, data := grant(cookies, csrf, aliceID, "", "", "ADMIN"); response.StatusCode != http.StatusConflict ||
		!bytes.Contains(data, []byte(`"code":"policy_conflict"`)) {
		t.Fatalf("stale grant status=%d body=%s", response.StatusCode, data)
	}
	if response, data := grant(cookies, csrf, adminID, "", "ADMIN", ""); response.StatusCode != http.StatusForbidden ||
		!bytes.Contains(data, []byte(`"code":"self_access_change"`)) {
		t.Fatalf("self demotion status=%d body=%s", response.StatusCode, data)
	}
	if response, data := grant(cookies, csrf, "unknown-identity", "", "", "VIEWER"); response.StatusCode != http.StatusNotFound ||
		bytes.Contains(data, []byte("unknown-identity")) {
		t.Fatalf("unknown identity status=%d body=%s", response.StatusCode, data)
	}

	_, aliceCookies, aliceCSRF, _ := c.login(t, "alice", "alice temporary passphrase")
	response, _ = c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"alice temporary passphrase","newPassword":"alice replacement passphrase"}`, c.origin, aliceCookies, aliceCSRF)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("alice password change status=%d", response.StatusCode)
	}
	_, aliceCookies, aliceCSRF, _ = c.login(t, "alice", "alice replacement passphrase")
	if response, data := grant(aliceCookies, aliceCSRF, adminID, "cluster-a", "", "VIEWER"); response.StatusCode != http.StatusForbidden ||
		!bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("cluster ADMIN grant status=%d body=%s", response.StatusCode, data)
	}
	if response, _ := c.request(t, http.MethodPut, "/api/admin/group-mappings", mapping, c.origin, aliceCookies, aliceCSRF); response.StatusCode != http.StatusForbidden {
		t.Fatalf("cluster ADMIN mapping status=%d", response.StatusCode)
	}
	if response, _ := c.request(t, http.MethodGet, "/api/admin/access", "", c.origin, aliceCookies, ""); response.StatusCode != http.StatusForbidden {
		t.Fatalf("cluster ADMIN listing status=%d", response.StatusCode)
	}

	before, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, path, body, origin, csrf string
		cookies                        []*http.Cookie
		want                           int
	}{
		{"anonymous", "/api/admin/role-grants", `{"identityId":"x","clusterId":"","expectedRole":"","role":"VIEWER"}`, c.origin, csrf, nil, http.StatusUnauthorized},
		{"wrong origin", "/api/admin/role-grants", `{"identityId":"x","clusterId":"","expectedRole":"","role":"VIEWER"}`, "https://attacker.example", csrf, cookies, http.StatusForbidden},
		{"missing CSRF", "/api/admin/role-grants", `{"identityId":"x","clusterId":"","expectedRole":"","role":"VIEWER"}`, c.origin, "", cookies, http.StatusForbidden},
		{"missing field", "/api/admin/role-grants", `{"identityId":"x","clusterId":"","role":"VIEWER"}`, c.origin, csrf, cookies, http.StatusBadRequest},
		{"null field", "/api/admin/role-grants", `{"identityId":"x","clusterId":null,"expectedRole":"","role":"VIEWER"}`, c.origin, csrf, cookies, http.StatusBadRequest},
		{"forged actor", "/api/admin/role-grants", `{"identityId":"x","clusterId":"","expectedRole":"","role":"VIEWER","actorId":"y"}`, c.origin, csrf, cookies, http.StatusBadRequest},
		{"mapping missing group", "/api/admin/group-mappings", `{"issuer":"https://issuer.example","clusterId":"","expectedRole":"","role":"VIEWER"}`, c.origin, csrf, cookies, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if response, _ := c.request(t, http.MethodPut, tt.path, tt.body, tt.origin, tt.cookies, tt.csrf); response.StatusCode != tt.want {
				t.Fatalf("status=%d, want %d", response.StatusCode, tt.want)
			}
		})
	}
	after, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{Global: true, Limit: 1000})
	if err != nil || len(after) != len(before) {
		t.Fatalf("rejected HTTP requests reached the access services: %d new events, %v", len(after)-len(before), err)
	}
}

type failingRoleGrant struct{ err error }

func (f failingRoleGrant) Execute(context.Context, identity.LocalAdminActor, string, string, authorization.Role, authorization.Role) (identity.AccessChangeResult, error) {
	return identity.AccessChangeResult{OperationID: "operation-123"}, f.err
}

func TestAccessChangeHTTPErrorSemantics(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		err        error
		status     int
	}{
		{"unknown outcome", "access_change_outcome_unknown", identity.ErrAccessChangeUnknown, http.StatusServiceUnavailable},
		{"audit unavailable", "access_change_audit_unavailable", identity.ErrAccessChangeAudit, http.StatusServiceUnavailable},
		{"unexpected", "access_change_outcome_unknown", errors.New("internal-sensitive-value"), http.StatusServiceUnavailable},
		{"session revoked", "unauthorized", identity.ErrInvalidAdminSession, http.StatusUnauthorized},
		{"not global admin", "global_admin_required", identity.ErrGlobalAdminRequired, http.StatusForbidden},
		{"self change", "self_access_change", identity.ErrSelfAccessChange, http.StatusForbidden},
		{"conflict", "policy_conflict", identity.ErrPolicyConflict, http.StatusConflict},
		{"last admin", "last_global_admin", identity.ErrLastLocalGlobalAdmin, http.StatusConflict},
		{"invalid", "invalid_access_change", identity.ErrInvalidAccessChange, http.StatusBadRequest},
		{"missing target", "identity_not_found", identity.ErrAccessTargetNotFound, http.StatusNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestConsole(t)
			api := c.handler.(*consoleapi.SessionAPI)
			list, _ := identity.NewListAccessBindings(c.db, rand.Reader, time.Now)
			mappings, _ := identity.NewChangeGroupMapping(c.db, rand.Reader, time.Now)
			if err := api.EnableAccessAdministration(list, failingRoleGrant{err: tt.err}, mappings); err != nil {
				t.Fatal(err)
			}
			_, cookies, csrf := unrestrictedAdmin(t, c)
			response, data := c.request(t, http.MethodPut, "/api/admin/role-grants",
				`{"identityId":"target","clusterId":"","expectedRole":"","role":"VIEWER"}`, c.origin, cookies, csrf)
			if response.StatusCode != tt.status || !strings.Contains(string(data), `"code":"`+tt.code+`"`) ||
				!strings.Contains(string(data), `"operationId":"operation-123"`) || strings.Contains(string(data), "internal-sensitive-value") {
				t.Fatalf("status=%d body=%s", response.StatusCode, data)
			}
		})
	}
}
