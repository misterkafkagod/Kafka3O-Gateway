package api_test

import (
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

// TestAPI_C6_Unsupported502WithKafkaErrorName proves a KindUnsupported
// DescribeQuorum error (a ZooKeeper-mode cluster, in reality) maps to 502
// KAFKA_ERROR (TECH-SPEC C3). It cannot also assert on kafkaError.name
// through the fake: FailNext only ever injects a bare Kind (TECH-SPEC §4.3
// — the fake never carries a KafkaCode/KafkaName through any error, natural
// or injected), so that part of this name is exercised instead by
// TestEnvelope_ErrorShapeAndContentType (internal/api/errors), which proves
// the envelope renders kafkaError.name whenever a *kafka.Error carries one
// — exactly what the real franz adapter's wrapErr populates against a real
// broker's UNSUPPORTED_VERSION response.
func TestAPI_C6_Unsupported502WithKafkaErrorName(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.FailNext("DescribeQuorum", kafka.KindUnsupported)

	resp := gw.Get(t, "/v1/cluster/quorum")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "KAFKA_ERROR" {
		t.Errorf("code = %v, want KAFKA_ERROR", errBody["code"])
	}
}

func TestAPI_C7_ListsReassignments(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1)

	planResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments?dryRun=true", map[string]any{
		"reassignments": []map[string]any{{"topic": "t-demo", "partition": 0, "replicas": []int{1, 2, 3}}},
	})
	planBody := decodeBody(t, planResp)
	plan, _ := planBody["plan"].(map[string]any)
	token, _ := plan["planToken"].(string)
	if token == "" {
		t.Fatalf("dry-run plan.planToken missing: %v", planBody)
	}

	execResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments", map[string]any{
		"confirm":       token,
		"reassignments": []map[string]any{{"topic": "t-demo", "partition": 0, "replicas": []int{1, 2, 3}}},
	})
	execBody := decodeBody(t, execResp)
	if execResp.StatusCode != http.StatusOK {
		t.Fatalf("execute status = %d, want 200: %v", execResp.StatusCode, execBody)
	}

	resp := gw.Get(t, "/v1/cluster/reassignments")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	items, _ := body["items"].([]any)
	found := false
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["topic"] == "t-demo" && item["partition"] == 0.0 {
			found = true
		}
	}
	if !found {
		t.Errorf("items = %v, want t-demo/0 listed", items)
	}
}

func TestAPI_C8_LogDirs(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1)
	gw.Fake.SeedLogDir("t-demo", 0, 1, "/var/kafka/data", 2048)

	resp := gw.Get(t, "/v1/cluster/log-dirs")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	items, _ := body["items"].([]any)
	found := false
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["brokerId"] == 1.0 && item["logDir"] == "/var/kafka/data" {
			found = true
			if item["totalBytes"] != 2048.0 {
				t.Errorf("totalBytes = %v, want 2048", item["totalBytes"])
			}
		}
	}
	if !found {
		t.Errorf("items = %v, want a broker 1 /var/kafka/data entry", items)
	}
}

func TestAPI_C10_SecondsAboveCeiling400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1)

	resp := gw.Get(t, "/v1/cluster/throughput?topic=t-demo&seconds=999")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "BOUND_EXCEEDED" {
		t.Errorf("code = %v, want BOUND_EXCEEDED", errBody["code"])
	}
}

func TestAPI_C11_ExportShape(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-orders", 1)
	gw.Fake.SeedTopicConfigs("t-orders", kafka.ConfigEntry{Name: "retention.ms", Value: "60000", Source: kafka.SourceDynamic})

	resp := gw.Get(t, "/v1/cluster/export?pattern=^t-")
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["exportedAt"] == nil || body["exportedAt"] == "" {
		t.Error("exportedAt missing")
	}
	topics, _ := body["topics"].([]any)
	if len(topics) != 1 {
		t.Fatalf("topics = %v, want exactly one", topics)
	}
	topic, _ := topics[0].(map[string]any)
	if topic["name"] != "t-orders" {
		t.Errorf("topics[0].name = %v, want t-orders", topic["name"])
	}
	configs, _ := topic["configs"].(map[string]any)
	if configs["retention.ms"] != "60000" {
		t.Errorf("configs = %v, want retention.ms=60000", configs)
	}
}

func TestAPI_C5_ConfirmationMismatch400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedBroker(1, "b1", 9092, "")
	gw.Fake.SeedBrokerConfigs(1, kafka.ConfigEntry{Name: "log.retention.hours", Value: "168"})

	resp := gw.Do(t, http.MethodPatch, "/v1/cluster/brokers/1/config", map[string]any{
		"confirm": "wrong", "set": map[string]any{"log.retention.hours": "72"},
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

func TestAPI_C9_DryRunThenExecuteEachOp(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1)
	gw.Fake.SeedPartitionMeta("t-demo", 0, 1, []int32{1, 2}, []int32{1, 2})

	t.Run("reassign", func(t *testing.T) {
		planResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments?dryRun=true", map[string]any{
			"reassignments": []map[string]any{{"topic": "t-demo", "partition": 0, "replicas": []int{1, 2, 3}}},
		})
		planBody := decodeBody(t, planResp)
		if planResp.StatusCode != http.StatusOK {
			t.Fatalf("dry-run status = %d, want 200: %v", planResp.StatusCode, planBody)
		}
		plan, _ := planBody["plan"].(map[string]any)
		token, _ := plan["planToken"].(string)

		execResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments", map[string]any{
			"confirm":       token,
			"reassignments": []map[string]any{{"topic": "t-demo", "partition": 0, "replicas": []int{1, 2, 3}}},
		})
		execBody := decodeBody(t, execResp)
		if execResp.StatusCode != http.StatusOK {
			t.Fatalf("execute status = %d, want 200: %v", execResp.StatusCode, execBody)
		}
		if execBody["summary"].(map[string]any)["ok"] != 1.0 {
			t.Errorf("summary = %v, want ok:1", execBody["summary"])
		}
	})

	t.Run("cancel", func(t *testing.T) {
		planResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments/cancel?dryRun=true", map[string]any{
			"cancel": []map[string]any{{"topic": "t-demo", "partition": 0}},
		})
		planBody := decodeBody(t, planResp)
		if planResp.StatusCode != http.StatusOK {
			t.Fatalf("dry-run status = %d, want 200: %v", planResp.StatusCode, planBody)
		}
		plan, _ := planBody["plan"].(map[string]any)
		token, _ := plan["planToken"].(string)

		execResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments/cancel", map[string]any{
			"confirm": token,
			"cancel":  []map[string]any{{"topic": "t-demo", "partition": 0}},
		})
		execBody := decodeBody(t, execResp)
		if execResp.StatusCode != http.StatusOK {
			t.Fatalf("execute status = %d, want 200: %v", execResp.StatusCode, execBody)
		}
	})

	t.Run("elect", func(t *testing.T) {
		planResp := gw.Do(t, http.MethodPost, "/v1/cluster/elections?dryRun=true", map[string]any{
			"elect": map[string]any{"type": "PREFERRED", "partitions": []map[string]any{{"topic": "t-demo", "partition": 0}}},
		})
		planBody := decodeBody(t, planResp)
		if planResp.StatusCode != http.StatusOK {
			t.Fatalf("dry-run status = %d, want 200: %v", planResp.StatusCode, planBody)
		}
		plan, _ := planBody["plan"].(map[string]any)
		token, _ := plan["planToken"].(string)

		execResp := gw.Do(t, http.MethodPost, "/v1/cluster/elections", map[string]any{
			"confirm": token,
			"elect":   map[string]any{"type": "PREFERRED", "partitions": []map[string]any{{"topic": "t-demo", "partition": 0}}},
		})
		execBody := decodeBody(t, execResp)
		if execResp.StatusCode != http.StatusOK {
			t.Fatalf("execute status = %d, want 200: %v", execResp.StatusCode, execBody)
		}
		if execBody["summary"].(map[string]any)["ok"] != 1.0 {
			t.Errorf("summary = %v, want ok:1", execBody["summary"])
		}
	})
}

func TestAPI_C9_StaleToken400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 2)

	staleResp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments?dryRun=true", map[string]any{
		"reassignments": []map[string]any{{"topic": "t-demo", "partition": 0, "replicas": []int{1, 2}}},
	})
	staleBody := decodeBody(t, staleResp)
	stalePlan, _ := staleBody["plan"].(map[string]any)
	staleToken, _ := stalePlan["planToken"].(string)
	if staleToken == "" {
		t.Fatalf("dry-run plan.planToken missing: %v", staleBody)
	}

	resp := gw.Do(t, http.MethodPost, "/v1/cluster/reassignments", map[string]any{
		"confirm": staleToken,
		"reassignments": []map[string]any{
			{"topic": "t-demo", "partition": 0, "replicas": []int{1, 2}},
			{"topic": "t-demo", "partition": 1, "replicas": []int{2, 3}},
		},
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

func TestAPI_C12_DryRunThenApplyThenUnchanged(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)

	planResp := gw.Do(t, http.MethodPost, "/v1/batch/topics/apply?dryRun=true", map[string]any{
		"topics": []map[string]any{{"name": "t-new", "partitions": 1, "replicationFactor": 1}},
	})
	planBody := decodeBody(t, planResp)
	if planResp.StatusCode != http.StatusOK {
		t.Fatalf("dry-run status = %d, want 200: %v", planResp.StatusCode, planBody)
	}
	plan, _ := planBody["plan"].(map[string]any)
	create, _ := plan["create"].([]any)
	if len(create) != 1 {
		t.Fatalf("plan.create = %v, want [t-new]", plan["create"])
	}
	token, _ := plan["planToken"].(string)

	execResp := gw.Do(t, http.MethodPost, "/v1/batch/topics/apply", map[string]any{
		"confirm": token,
		"topics":  []map[string]any{{"name": "t-new", "partitions": 1, "replicationFactor": 1}},
	})
	execBody := decodeBody(t, execResp)
	if execResp.StatusCode != http.StatusOK {
		t.Fatalf("execute status = %d, want 200: %v", execResp.StatusCode, execBody)
	}

	againResp := gw.Do(t, http.MethodPost, "/v1/batch/topics/apply?dryRun=true", map[string]any{
		"topics": []map[string]any{{"name": "t-new", "partitions": 1, "replicationFactor": 1}},
	})
	againBody := decodeBody(t, againResp)
	if againResp.StatusCode != http.StatusOK {
		t.Fatalf("second dry-run status = %d, want 200: %v", againResp.StatusCode, againBody)
	}
	againPlan, _ := againBody["plan"].(map[string]any)
	unchanged, _ := againPlan["unchanged"].([]any)
	if len(unchanged) != 1 || unchanged[0] != "t-new" {
		t.Errorf("plan.unchanged = %v, want [t-new]", againPlan["unchanged"])
	}
}
