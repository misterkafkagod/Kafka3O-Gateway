//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_M5_Produce(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/messages", operatorKey, map[string]any{
		"records": []map[string]any{{"value": "acceptance-" + runID}},
	})
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Items []struct {
			Status    string `json:"status"`
			Partition int32  `json:"partition"`
			Offset    int64  `json:"offset"`
		} `json:"items"`
		Summary struct {
			Total int `json:"total"`
			OK    int `json:"ok"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Summary.Total != 1 || body.Summary.OK != 1 {
		t.Fatalf("summary = %+v, want {total:1 ok:1}", body.Summary)
	}
	if len(body.Items) != 1 || body.Items[0].Status != "ok" {
		t.Fatalf("items = %+v, want one ok item", body.Items)
	}
}

func TestAcceptance_M6_ProduceBulk(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	ndjson := []byte("{\"value\":\"acceptance-" + runID + "-a\"}\n{\"value\":\"acceptance-" + runID + "-b\"}\n")
	resp := doAcceptanceRaw(t, http.MethodPost, "/v1/topics/"+topic+"/messages/bulk", operatorKey, "application/x-ndjson", ndjson)
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

func TestAcceptance_M7_Tombstone(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/tombstones", operatorKey, map[string]any{
		"key": "acceptance-tombstone-" + runID,
	})
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Offset < 0 {
		t.Errorf("offset = %d, want >= 0", body.Offset)
	}
}
