package api_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_T5_201Then409(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Do(t, http.MethodPost, "/v1/topics", map[string]any{"name": "t-new", "partitions": 2, "replicationFactor": 1})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %v", resp.StatusCode, body)
	}
	if body["name"] != "t-new" || body["partitions"] != 2.0 {
		t.Errorf("body = %v, want name t-new, partitions 2", body)
	}

	resp2 := gw.Do(t, http.MethodPost, "/v1/topics", map[string]any{"name": "t-new", "partitions": 2, "replicationFactor": 1})
	body2 := decodeBody(t, resp2)
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("repeat status = %d, want 409: %v", resp2.StatusCode, body2)
	}
	errBody, _ := body2["error"].(map[string]any)
	if errBody["code"] != "ALREADY_EXISTS" {
		t.Errorf("repeat code = %v, want ALREADY_EXISTS", errBody["code"])
	}
}

func TestAPI_T5_DryRun200PlanTopicAbsent(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	resp := gw.Do(t, http.MethodPost, "/v1/topics?dryRun=true",
		map[string]any{"name": "t-dry", "partitions": 3, "replicationFactor": 1})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["dryRun"] != true {
		t.Errorf("dryRun = %v, want true", body["dryRun"])
	}
	plan, ok := body["plan"].(map[string]any)
	if !ok || plan["name"] != "t-dry" {
		t.Fatalf("plan = %v, want name t-dry", body["plan"])
	}

	getResp := gw.Get(t, "/v1/topics/t-dry")
	getBody := decodeBody(t, getResp)
	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /v1/topics/t-dry after a dry-run create = %d, want 404 (nothing was created): %v", getResp.StatusCode, getBody)
	}
}

func TestAPI_T6_400OnInvalidItem(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-existing", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/batch/topics", map[string]any{
		"topics": []map[string]any{
			{"name": "t-fresh", "partitions": 1, "replicationFactor": 1},
			{"name": "t-existing", "partitions": 1, "replicationFactor": 1},
		},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "BULK_VALIDATION_FAILED" {
		t.Errorf("code = %v, want BULK_VALIDATION_FAILED", errBody["code"])
	}

	if _, err := gw.Fake.DescribeTopics(context.Background(), "t-fresh"); err == nil {
		t.Error("t-fresh exists after a failed bulk create, want nothing created")
	}
}

func TestAPI_T9_ConfirmationMismatch400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-cfg", 1)

	resp := gw.Do(t, http.MethodPatch, "/v1/topics/t-cfg/config", map[string]any{
		"confirm": "wrong-name", "set": map[string]any{"retention.ms": "60000"},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "CONFIRMATION_MISMATCH" {
		t.Errorf("code = %v, want CONFIRMATION_MISMATCH", errBody["code"])
	}
}

func TestAPI_T9_DryRunPlan(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-cfg", 1)

	resp := gw.Do(t, http.MethodPatch, "/v1/topics/t-cfg/config?dryRun=true", map[string]any{
		"confirm": "t-cfg", "set": map[string]any{"retention.ms": "60000"},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["dryRun"] != true {
		t.Errorf("dryRun = %v, want true", body["dryRun"])
	}
	plan, ok := body["plan"].(map[string]any)
	if !ok || plan["topic"] != "t-cfg" {
		t.Fatalf("plan = %v, want topic t-cfg", body["plan"])
	}
	changes, _ := plan["changes"].([]any)
	if len(changes) != 1 {
		t.Fatalf("plan.changes = %v, want one change", changes)
	}
}

func TestAPI_T10_OperationDisabled403OnlyThatOp(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithDisabledOperations("T10"))
	gw.Fake.SeedTopic("t-parts", 2)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t-parts/partitions", map[string]any{"confirm": "t-parts", "partitions": 4})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("T10 status = %d, want 403: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "OPERATION_DISABLED" {
		t.Errorf("T10 code = %v, want OPERATION_DISABLED", errBody["code"])
	}

	configResp := gw.Do(t, http.MethodPatch, "/v1/topics/t-parts/config", map[string]any{
		"confirm": "t-parts", "set": map[string]any{"retention.ms": "60000"},
	})
	configBody := decodeBody(t, configResp)
	if configResp.StatusCode != http.StatusOK {
		t.Fatalf("T9 status with only T10 disabled = %d, want 200: %v", configResp.StatusCode, configBody)
	}
}

func TestAPI_T9T10_ResultSeverityInfo(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-cfg", 1)
	gw.Fake.SeedTopic("t-parts", 1)

	resp := gw.Do(t, http.MethodPatch, "/v1/topics/t-cfg/config", map[string]any{
		"confirm": "t-cfg", "set": map[string]any{"retention.ms": "60000"},
	})
	_ = decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("T9 status = %d, want 200", resp.StatusCode)
	}

	resp2 := gw.Do(t, http.MethodPost, "/v1/topics/t-parts/partitions", map[string]any{"confirm": "t-parts", "partitions": 4})
	_ = decodeBody(t, resp2)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("T10 status = %d, want 200", resp2.StatusCode)
	}

	events := gw.Audit.Events()
	var t9Result, t10Result *audit.Event
	for i := range events {
		ev := events[i]
		if ev.Phase != audit.PhaseResult {
			continue
		}
		switch ev.CommandID {
		case "T9":
			t9Result = &events[i]
		case "T10":
			t10Result = &events[i]
		}
	}
	if t9Result == nil || t9Result.Severity != audit.SeverityInfo {
		t.Errorf("T9 RESULT = %+v, want Severity INFO", t9Result)
	}
	if t10Result == nil || t10Result.Severity != audit.SeverityInfo {
		t.Errorf("T10 RESULT = %+v, want Severity INFO", t10Result)
	}
}
