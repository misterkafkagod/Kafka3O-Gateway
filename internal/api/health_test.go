package api_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func getJSON(t *testing.T, gw *testutil.Gateway, path string) (int, map[string]any) {
	t.Helper()
	resp := gw.Get(t, path)
	defer func() { _ = resp.Body.Close() }()

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return resp.StatusCode, body
}

func TestHealth_LiveAlways200(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithUnreachable())

	status, body := getJSON(t, gw, "/v1/health/live")

	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body["status"] != "UP" {
		t.Errorf("status field = %v, want UP", body["status"])
	}
}

func TestHealth_Ready503WhenFakeUnreachable(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithUnreachable())

	status, body := getJSON(t, gw, "/v1/health/ready")

	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	if body["status"] != "DOWN" {
		t.Errorf("status field = %v, want DOWN", body["status"])
	}
	cluster, ok := body["cluster"].(map[string]any)
	if !ok || cluster["reachable"] != false {
		t.Errorf("cluster = %v, want reachable:false", body["cluster"])
	}
}

func TestHealth_ReadyReportsAuditFieldWithoutGating(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithUnreachable())

	status, body := getJSON(t, gw, "/v1/health/ready")

	// The cluster is unreachable (503 DOWN), yet the audit sub-object is
	// still present and reports healthy: audit health never gates
	// readiness (TECH-SPEC §6.1 B5).
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	audit, ok := body["audit"].(map[string]any)
	if !ok {
		t.Fatalf("audit field missing: %v", body)
	}
	if audit["sink"] != "stdout" || audit["healthy"] != true {
		t.Errorf("audit = %v, want {sink:stdout,healthy:true}", audit)
	}
}
