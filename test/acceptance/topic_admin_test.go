//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_T5_CreateTopic(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-t5"

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Name != topic {
		t.Errorf("name = %q, want %q", body.Name, topic)
	}

	resp2 := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	defer drainAndClose(resp2)
	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("repeat status = %d, want 409", resp2.StatusCode)
	}
}

func TestAcceptance_T6_CreateTopicsBulk(t *testing.T) {
	t.Parallel()
	topicA := "acc-" + runID + "-t6-a"
	topicB := "acc-" + runID + "-t6-b"

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/batch/topics", operatorKey, map[string]any{
		"topics": []map[string]any{
			{"name": topicA, "partitions": 1, "replicationFactor": 1},
			{"name": topicB, "partitions": 1, "replicationFactor": 1},
		},
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

func TestAcceptance_T9_AlterConfig(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-t9"
	createResp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	drainAndClose(createResp)
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createResp.StatusCode)
	}

	resp := doAcceptanceJSON(t, http.MethodPatch, "/v1/topics/"+topic+"/config", operatorKey, map[string]any{
		"confirm": topic, "set": map[string]any{"retention.ms": "60000"},
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Configs []struct {
			Name   string `json:"name"`
			Value  string `json:"value"`
			Source string `json:"source"`
		} `json:"configs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	found := false
	for _, c := range body.Configs {
		if c.Name == "retention.ms" {
			found = true
			if c.Value != "60000" || c.Source != "dynamic" {
				t.Errorf("retention.ms = %+v, want value 60000 source dynamic", c)
			}
		}
	}
	if !found {
		t.Errorf("configs = %+v, want retention.ms present", body.Configs)
	}
}

func TestAcceptance_T10_AddPartitions(t *testing.T) {
	t.Parallel()
	topic := "acc-" + runID + "-t10"
	createResp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	drainAndClose(createResp)
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createResp.StatusCode)
	}

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/partitions", operatorKey, map[string]any{
		"confirm": topic, "partitions": 3,
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		PartitionCount int32 `json:"partitionCount"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.PartitionCount != 3 {
		t.Errorf("partitionCount = %d, want 3", body.PartitionCount)
	}
}
