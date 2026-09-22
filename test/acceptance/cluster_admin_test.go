//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
)

// describeCluster fetches C1's own response, used by several tests below to
// discover a real broker id and the cluster's current topology rather than
// hardcoding one.
func describeCluster(t *testing.T) []int {
	t.Helper()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster", operatorKey)
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/cluster status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Brokers []struct {
			ID int `json:"id"`
		} `json:"brokers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	ids := make([]int, len(body.Brokers))
	for i, b := range body.Brokers {
		ids[i] = b.ID
	}
	return ids
}

func TestAcceptance_C5_AlterBrokerConfig(t *testing.T) {
	t.Parallel()
	brokers := describeCluster(t)
	if len(brokers) == 0 {
		t.Skip("cluster reports no brokers")
	}
	brokerID := brokers[0]

	confirm := strconv.Itoa(brokerID)
	resp := doAcceptanceJSON(t, http.MethodPatch, "/v1/cluster/brokers/"+confirm+"/config", operatorKey, map[string]any{
		"confirm": confirm, "set": map[string]any{"log.retention.hours": "168"},
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAcceptance_C6_Quorum(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/quorum", operatorKey)
	defer drainAndClose(resp)
	// A ZooKeeper-mode cluster legitimately reports 502 KAFKA_ERROR here
	// (TECH-SPEC C3) — this suite only proves the endpoint round-trips, not
	// that the target cluster runs KRaft.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 200 or 502", resp.StatusCode)
	}
}

func TestAcceptance_C7_Reassignments(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/reassignments", operatorKey)
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAcceptance_C8_LogDirs(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/log-dirs", operatorKey)
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Items []any `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) == 0 {
		t.Error("items = [], want at least one broker log directory")
	}
}

func TestAcceptance_C9_ReassignCancelElect(t *testing.T) {
	t.Parallel()
	brokers := describeCluster(t)
	if len(brokers) < 2 {
		t.Skip("cluster has fewer than 2 brokers; C9 reassign needs a real target broker")
	}
	topic := createAcceptanceTopic(t, "c9", 0)

	// Reassign partition 0 onto the first two brokers, in reverse order —
	// always a genuine change regardless of which broker originally led.
	target := []int{brokers[1], brokers[0]}

	planResp := doAcceptanceJSON(t, http.MethodPost, "/v1/cluster/reassignments?dryRun=true", operatorKey, map[string]any{
		"reassignments": []map[string]any{{"topic": topic, "partition": 0, "replicas": target}},
	})
	var planBody struct {
		Plan struct {
			PlanToken string `json:"planToken"`
		} `json:"plan"`
	}
	if err := json.NewDecoder(planResp.Body).Decode(&planBody); err != nil {
		drainAndClose(planResp)
		t.Fatalf("decode dry-run response: %v", err)
	}
	drainAndClose(planResp)
	if planResp.StatusCode != http.StatusOK || planBody.Plan.PlanToken == "" {
		t.Fatalf("dry-run status = %d, plan = %+v, want 200 with a plan token", planResp.StatusCode, planBody.Plan)
	}

	execResp := doAcceptanceJSON(t, http.MethodPost, "/v1/cluster/reassignments", operatorKey, map[string]any{
		"confirm":       planBody.Plan.PlanToken,
		"reassignments": []map[string]any{{"topic": topic, "partition": 0, "replicas": target}},
	})
	drainAndClose(execResp)
	if execResp.StatusCode != http.StatusOK {
		t.Fatalf("reassign status = %d, want 200", execResp.StatusCode)
	}

	electPlanResp := doAcceptanceJSON(t, http.MethodPost, "/v1/cluster/elections?dryRun=true", operatorKey, map[string]any{
		"elect": map[string]any{"type": "PREFERRED", "partitions": []map[string]any{{"topic": topic, "partition": 0}}},
	})
	var electPlanBody struct {
		Plan struct {
			PlanToken string `json:"planToken"`
		} `json:"plan"`
	}
	if err := json.NewDecoder(electPlanResp.Body).Decode(&electPlanBody); err != nil {
		drainAndClose(electPlanResp)
		t.Fatalf("decode elect dry-run response: %v", err)
	}
	drainAndClose(electPlanResp)

	electResp := doAcceptanceJSON(t, http.MethodPost, "/v1/cluster/elections", operatorKey, map[string]any{
		"confirm": electPlanBody.Plan.PlanToken,
		"elect":   map[string]any{"type": "PREFERRED", "partitions": []map[string]any{{"topic": topic, "partition": 0}}},
	})
	drainAndClose(electResp)
	if electResp.StatusCode != http.StatusOK {
		t.Fatalf("elect status = %d, want 200", electResp.StatusCode)
	}

	cancelPlanResp := doAcceptanceJSON(t, http.MethodPost, "/v1/cluster/reassignments/cancel?dryRun=true", operatorKey, map[string]any{
		"cancel": []map[string]any{{"topic": topic, "partition": 0}},
	})
	var cancelPlanBody struct {
		Plan struct {
			PlanToken string `json:"planToken"`
		} `json:"plan"`
	}
	if err := json.NewDecoder(cancelPlanResp.Body).Decode(&cancelPlanBody); err != nil {
		drainAndClose(cancelPlanResp)
		t.Fatalf("decode cancel dry-run response: %v", err)
	}
	drainAndClose(cancelPlanResp)

	cancelResp := doAcceptanceJSON(t, http.MethodPost, "/v1/cluster/reassignments/cancel", operatorKey, map[string]any{
		"confirm": cancelPlanBody.Plan.PlanToken,
		"cancel":  []map[string]any{{"topic": topic, "partition": 0}},
	})
	drainAndClose(cancelResp)
	if cancelResp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", cancelResp.StatusCode)
	}
}

func TestAcceptance_C10_Throughput(t *testing.T) {
	t.Parallel()
	topic := createAcceptanceTopic(t, "c10", 2)

	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/throughput?topic="+topic+"&seconds=3", operatorKey)
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Seconds int `json:"seconds"`
		Items   []struct {
			Topic string `json:"topic"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Seconds != 3 || len(body.Items) != 1 || body.Items[0].Topic != topic {
		t.Fatalf("body = %+v, want seconds 3 and one item for %s", body, topic)
	}
}

func TestAcceptance_C11_Export(t *testing.T) {
	t.Parallel()
	topic := createAcceptanceTopic(t, "c11", 0)

	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/export?pattern=^"+topic+"$", operatorKey)
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		ExportedAt string `json:"exportedAt"`
		Topics     []struct {
			Name string `json:"name"`
		} `json:"topics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ExportedAt == "" || len(body.Topics) != 1 || body.Topics[0].Name != topic {
		t.Fatalf("body = %+v, want exportedAt set and exactly [%s]", body, topic)
	}
}

func TestAcceptance_C12_Import(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-c12"

	planResp := doAcceptanceJSON(t, http.MethodPost, "/v1/batch/topics/apply?dryRun=true", operatorKey, map[string]any{
		"topics": []map[string]any{{"name": topic, "partitions": 1, "replicationFactor": 1}},
	})
	var planBody struct {
		Plan struct {
			Create    []map[string]any `json:"create"`
			PlanToken string           `json:"planToken"`
		} `json:"plan"`
	}
	if err := json.NewDecoder(planResp.Body).Decode(&planBody); err != nil {
		drainAndClose(planResp)
		t.Fatalf("decode dry-run response: %v", err)
	}
	drainAndClose(planResp)
	if planResp.StatusCode != http.StatusOK || len(planBody.Plan.Create) != 1 {
		t.Fatalf("dry-run status = %d, plan = %+v, want 200 with one create entry", planResp.StatusCode, planBody.Plan)
	}

	execResp := doAcceptanceJSON(t, http.MethodPost, "/v1/batch/topics/apply", operatorKey, map[string]any{
		"confirm": planBody.Plan.PlanToken,
		"topics":  []map[string]any{{"name": topic, "partitions": 1, "replicationFactor": 1}},
	})
	drainAndClose(execResp)
	if execResp.StatusCode != http.StatusOK {
		t.Fatalf("apply status = %d, want 200", execResp.StatusCode)
	}

	describeResp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic, operatorKey)
	drainAndClose(describeResp)
	if describeResp.StatusCode != http.StatusOK {
		t.Errorf("GET /v1/topics/%s after import = %d, want 200", topic, describeResp.StatusCode)
	}
}
