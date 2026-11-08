package httpapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	consoleapi "github.com/misterkafkagod/Kafka3O-Gateway/internal/console/httpapi"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/storage"
)

func enableTestClusterAdministration(t *testing.T, c testConsole) {
	t.Helper()
	list, err := identity.NewListRegisteredClusters(c.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	classify, err := identity.NewChangeClusterEnvironment(c.db, rand.Reader, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.handler.(*consoleapi.SessionAPI).EnableClusterAdministration(list, classify); err != nil {
		t.Fatal(err)
	}
	if err := c.db.SyncRegisteredClusters(context.Background(), []identity.RegisteredCluster{
		{ID: "dev", Name: "Dev", GatewayURL: "https://dev.example:8081"}, {ID: "prod", Name: "Prod", GatewayURL: "https://prod.example:8082"},
	}, "test-registry", time.Now()); err != nil {
		t.Fatal(err)
	}
}

type clusterListing struct {
	PolicyRevision int64 `json:"policyRevision"`
	Clusters       []struct {
		ID, Name, GatewayURL, Environment string
		Active                            bool
	} `json:"clusters"`
}

func listClusters(t *testing.T, c testConsole, cookies []*http.Cookie) clusterListing {
	t.Helper()
	response, data := c.request(t, http.MethodGet, "/api/admin/clusters", "", c.origin, cookies, "")
	var listing clusterListing
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" || json.Unmarshal(data, &listing) != nil {
		t.Fatalf("cluster listing status=%d body=%s", response.StatusCode, data)
	}
	return listing
}

func TestClusterClassificationHTTPSFlow(t *testing.T) {
	c := newTestConsole(t)
	enableTestClusterAdministration(t, c)
	enableTestAccessAdministration(t, c)
	_, cookies, csrf := unrestrictedAdmin(t, c)
	listing := listClusters(t, c, cookies)
	if len(listing.Clusters) != 2 || listing.Clusters[0].ID != "dev" || listing.Clusters[0].Environment != "" ||
		listing.Clusters[1].GatewayURL != "https://prod.example:8082" || !listing.Clusters[1].Active {
		t.Fatalf("initial clusters = %+v", listing)
	}
	classify := func(cookies []*http.Cookie, csrf, path, body string) (*http.Response, []byte) {
		t.Helper()
		return c.request(t, http.MethodPut, path, body, c.origin, cookies, csrf)
	}
	for _, tt := range []struct {
		name, path, body, code string
		status                 int
	}{
		{"missing reason field", "/api/admin/clusters/prod/environment", `{"expectedEnvironment":"","environment":"PRODUCTION"}`, "invalid_request", http.StatusBadRequest},
		{"empty reason", "/api/admin/clusters/prod/environment", `{"expectedEnvironment":"","environment":"PRODUCTION","reason":""}`, "change_reason_required", http.StatusBadRequest},
		{"stale expected", "/api/admin/clusters/prod/environment", `{"expectedEnvironment":"NON_PRODUCTION","environment":"PRODUCTION","reason":"r"}`, "policy_conflict", http.StatusConflict},
		{"unknown cluster", "/api/admin/clusters/unknown/environment", `{"expectedEnvironment":"","environment":"PRODUCTION","reason":"r"}`, "cluster_not_found", http.StatusNotFound},
		{"unclassify", "/api/admin/clusters/prod/environment", `{"expectedEnvironment":"","environment":"","reason":"r"}`, "invalid_access_change", http.StatusBadRequest},
		{"query parameter", "/api/admin/clusters/prod/environment?environment=NON_PRODUCTION", `{"expectedEnvironment":"","environment":"PRODUCTION","reason":"r"}`, "invalid_request", http.StatusBadRequest},
		{"forged field", "/api/admin/clusters/prod/environment", `{"expectedEnvironment":"","environment":"PRODUCTION","reason":"r","name":"Production"}`, "invalid_request", http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response, data := classify(cookies, csrf, tt.path, tt.body)
			if response.StatusCode != tt.status || !bytes.Contains(data, []byte(`"code":"`+tt.code+`"`)) {
				t.Fatalf("status=%d body=%s", response.StatusCode, data)
			}
		})
	}
	if response, data := classify(cookies, csrf, "/api/admin/clusters/prod/environment",
		`{"expectedEnvironment":"","environment":"PRODUCTION","reason":"Customer-facing cluster"}`); response.StatusCode != http.StatusOK ||
		!bytes.Contains(data, []byte(`"policyRevision":1`)) {
		t.Fatalf("classify status=%d body=%s", response.StatusCode, data)
	}
	if got := listClusters(t, c, cookies); got.PolicyRevision != 1 || got.Clusters[1].Environment != "PRODUCTION" || got.Clusters[0].Environment != "" {
		t.Fatalf("clusters after classification = %+v", got)
	}

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
	grant, _ := json.Marshal(map[string]string{"identityId": created.Account.ID, "clusterId": "prod", "expectedRole": "", "role": "ADMIN"})
	if response, _ := c.request(t, http.MethodPut, "/api/admin/role-grants", string(grant), c.origin, cookies, csrf); response.StatusCode != http.StatusOK {
		t.Fatalf("grant prod ADMIN status=%d", response.StatusCode)
	}
	unregistered, _ := json.Marshal(map[string]string{"identityId": created.Account.ID, "clusterId": "unknown", "expectedRole": "", "role": "VIEWER"})
	if response, data := c.request(t, http.MethodPut, "/api/admin/role-grants", string(unregistered), c.origin, cookies, csrf); response.StatusCode != http.StatusNotFound ||
		!bytes.Contains(data, []byte(`"code":"cluster_not_found"`)) {
		t.Fatalf("unregistered cluster grant status=%d body=%s", response.StatusCode, data)
	}
	_, aliceCookies, aliceCSRF, _ := c.login(t, "alice", "alice temporary passphrase")
	if response, _ := c.request(t, http.MethodPost, "/api/session/password",
		`{"currentPassword":"alice temporary passphrase","newPassword":"alice replacement passphrase"}`, c.origin, aliceCookies, aliceCSRF); response.StatusCode != http.StatusOK {
		t.Fatal("alice password change failed")
	}
	_, aliceCookies, aliceCSRF, _ = c.login(t, "alice", "alice replacement passphrase")
	if response, data := classify(aliceCookies, aliceCSRF, "/api/admin/clusters/prod/environment",
		`{"expectedEnvironment":"PRODUCTION","environment":"NON_PRODUCTION","reason":"downgrade"}`); response.StatusCode != http.StatusForbidden ||
		!bytes.Contains(data, []byte(`"code":"global_admin_required"`)) {
		t.Fatalf("cluster ADMIN downgrade status=%d body=%s", response.StatusCode, data)
	}
	if response, _ := c.request(t, http.MethodGet, "/api/admin/clusters", "", c.origin, aliceCookies, ""); response.StatusCode != http.StatusForbidden {
		t.Fatalf("cluster ADMIN listing status=%d", response.StatusCode)
	}
	if environment, err := c.db.ClusterEnvironment(context.Background(), "prod"); err != nil || environment != "PRODUCTION" {
		t.Fatalf("prod environment after denied downgrade = %q, %v", environment, err)
	}
	events, err := c.db.ListAuditEvents(context.Background(), storage.AuditFilter{ClusterID: "prod", Action: "console.cluster.classify", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var succeeded, denied int
	for _, event := range events {
		switch event.Outcome {
		case storage.AuditSucceeded:
			succeeded++
			if event.Reason != "Customer-facing cluster" || event.BeforeState != "" || event.AfterState != "PRODUCTION" {
				t.Fatalf("classification audit = %+v", event)
			}
		case storage.AuditDenied:
			denied++
		}
	}
	if succeeded != 1 {
		t.Fatalf("classification audit counts: succeeded=%d denied=%d", succeeded, denied)
	}
}
