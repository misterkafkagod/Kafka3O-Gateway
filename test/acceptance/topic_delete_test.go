//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_T7_DeleteTopic(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-t7"
	createResp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	drainAndClose(createResp)
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createResp.StatusCode)
	}

	resp := doAcceptanceJSON(t, http.MethodDelete, "/v1/topics/"+topic, operatorKey, map[string]any{"confirm": topic})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Deleted string `json:"deleted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Deleted != topic {
		t.Errorf("deleted = %q, want %q", body.Deleted, topic)
	}

	getResp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic, operatorKey)
	drainAndClose(getResp)
	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("GET %s after delete = %d, want 404", topic, getResp.StatusCode)
	}
}

func TestAcceptance_T8_BulkDelete(t *testing.T) {
	t.Parallel()
	prefix := "acc-" + runID + "-t8-"
	topicA := prefix + "a"
	topicB := prefix + "b"
	for _, name := range []string{topicA, topicB} {
		resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
			"name": name, "partitions": 1, "replicationFactor": 1,
		})
		drainAndClose(resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %s status = %d, want 201", name, resp.StatusCode)
		}
	}

	planResp := doAcceptanceJSON(t, http.MethodPost, "/v1/batch/topics/delete?dryRun=true", operatorKey, map[string]any{
		"pattern": "^" + prefix,
	})
	var planBody struct {
		Plan struct {
			Topics    []string `json:"topics"`
			PlanToken string   `json:"planToken"`
		} `json:"plan"`
	}
	if err := json.NewDecoder(planResp.Body).Decode(&planBody); err != nil {
		drainAndClose(planResp)
		t.Fatalf("decode dry-run response: %v", err)
	}
	drainAndClose(planResp)
	if planResp.StatusCode != http.StatusOK || len(planBody.Plan.Topics) != 2 || planBody.Plan.PlanToken == "" {
		t.Fatalf("dry-run plan = %+v (status %d), want two topics and a plan token", planBody.Plan, planResp.StatusCode)
	}

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/batch/topics/delete", operatorKey, map[string]any{
		"confirm": planBody.Plan.PlanToken, "pattern": "^" + prefix,
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Summary struct {
			Total int `json:"total"`
			OK    int `json:"ok"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Summary.Total != 2 || body.Summary.OK != 2 {
		t.Fatalf("summary = %+v, want {total:2 ok:2}", body.Summary)
	}
}

func TestAcceptance_T11_DeleteRecords(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-t11"
	createResp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	drainAndClose(createResp)
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createResp.StatusCode)
	}

	for range 5 {
		resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/messages", operatorKey, map[string]any{
			"records": []map[string]any{{"value": "v"}},
		})
		drainAndClose(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("produce status = %d, want 200", resp.StatusCode)
		}
	}

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/delete-records", operatorKey, map[string]any{
		"confirm": topic, "offsets": map[string]any{"0": 3},
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Partitions []struct {
			ID           int32 `json:"id"`
			LowWatermark int64 `json:"lowWatermark"`
		} `json:"partitions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Partitions) != 1 || body.Partitions[0].LowWatermark != 3 {
		t.Fatalf("partitions = %+v, want one entry with lowWatermark 3", body.Partitions)
	}
}

func TestAcceptance_T12_Purge(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-t12"
	createResp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	drainAndClose(createResp)
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createResp.StatusCode)
	}

	for range 5 {
		resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/messages", operatorKey, map[string]any{
			"records": []map[string]any{{"value": "v"}},
		})
		drainAndClose(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("produce status = %d, want 200", resp.StatusCode)
		}
	}

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/purge", operatorKey, map[string]any{"confirm": topic})
	drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("purge status = %d, want 200", resp.StatusCode)
	}

	readResp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic+"/messages?from=beginning", operatorKey)
	defer drainAndClose(readResp)
	var body struct {
		Items []any `json:"items"`
		Scan  struct {
			ReachedEnd bool `json:"reachedEnd"`
		} `json:"scan"`
	}
	if err := json.NewDecoder(readResp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) != 0 || !body.Scan.ReachedEnd {
		t.Fatalf("read after purge = %+v, want empty items and reachedEnd true", body)
	}
}
