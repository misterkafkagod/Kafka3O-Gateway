//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

// createAcceptanceTopic creates a fresh single-partition topic via T5 and
// produces n records into it, returning the topic name — self-contained
// setup for G4/G7's acceptance tests, which need real committed offsets to
// reset or clone but not a pre-existing cluster fixture.
func createAcceptanceTopic(t *testing.T, suffix string, records int) string {
	t.Helper()
	topic := "acc-" + runID + "-" + suffix
	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
		"name": topic, "partitions": 1, "replicationFactor": 1,
	})
	drainAndClose(resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create %s status = %d, want 201", topic, resp.StatusCode)
	}
	for range records {
		r := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/messages", operatorKey, map[string]any{
			"records": []map[string]any{{"value": "v"}},
		})
		drainAndClose(r)
		if r.StatusCode != http.StatusOK {
			t.Fatalf("produce to %s status = %d, want 200", topic, r.StatusCode)
		}
	}
	return topic
}

func TestAcceptance_G4_ResetOffsets(t *testing.T) {
	t.Parallel()
	topic := createAcceptanceTopic(t, "g4", 5)
	group := "acc-" + runID + "-g4-group"

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/consumer-groups/"+group+"/reset-offsets", operatorKey, map[string]any{
		"confirm": group, "target": map[string]any{"mode": "latest"}, "topics": []string{topic},
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Offsets []struct {
			Topic string `json:"topic"`
			After int64  `json:"after"`
		} `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Offsets) != 1 || body.Offsets[0].After != 5 {
		t.Fatalf("offsets = %+v, want one entry with after=5", body.Offsets)
	}

	describeResp := doAcceptance(t, http.MethodGet, "/v1/consumer-groups/"+group, operatorKey)
	defer drainAndClose(describeResp)
	if describeResp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", group, describeResp.StatusCode)
	}
}

func TestAcceptance_G5_DeleteGroup(t *testing.T) {
	t.Parallel()
	topic := createAcceptanceTopic(t, "g5", 1)
	group := "acc-" + runID + "-g5-group"

	seedResp := doAcceptanceJSON(t, http.MethodPost, "/v1/consumer-groups/"+group+"/reset-offsets", operatorKey, map[string]any{
		"confirm": group, "target": map[string]any{"mode": "earliest"}, "topics": []string{topic},
	})
	drainAndClose(seedResp)
	if seedResp.StatusCode != http.StatusOK {
		t.Fatalf("pre-seed status = %d, want 200", seedResp.StatusCode)
	}

	resp := doAcceptanceJSON(t, http.MethodDelete, "/v1/consumer-groups/"+group, operatorKey, map[string]any{"confirm": group})
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
	if body.Deleted != group {
		t.Errorf("deleted = %q, want %q", body.Deleted, group)
	}
}

func TestAcceptance_G6_RemoveMembers(t *testing.T) {
	t.Parallel()
	topic := createAcceptanceTopic(t, "g6", 1)
	group := "acc-" + runID + "-g6-group"

	seedResp := doAcceptanceJSON(t, http.MethodPost, "/v1/consumer-groups/"+group+"/reset-offsets", operatorKey, map[string]any{
		"confirm": group, "target": map[string]any{"mode": "earliest"}, "topics": []string{topic},
	})
	drainAndClose(seedResp)
	if seedResp.StatusCode != http.StatusOK {
		t.Fatalf("pre-seed status = %d, want 200", seedResp.StatusCode)
	}

	// No live member ever joined this group, so removing "all" (an omitted
	// members list) is a clean no-op round trip — real eviction of an
	// active member is covered by Phase 10's manual test plan, which needs
	// a genuinely running consumer process.
	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/consumer-groups/"+group+"/remove-members", operatorKey, map[string]any{"confirm": group})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Removed []string `json:"removed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Removed) != 0 {
		t.Errorf("removed = %v, want none (no live members to evict)", body.Removed)
	}
}

func TestAcceptance_G7_CloneOffsets(t *testing.T) {
	t.Parallel()
	topic := createAcceptanceTopic(t, "g7", 7)
	source := "acc-" + runID + "-g7-source"
	target := "acc-" + runID + "-g7-target"

	seedResp := doAcceptanceJSON(t, http.MethodPost, "/v1/consumer-groups/"+source+"/reset-offsets", operatorKey, map[string]any{
		"confirm": source, "target": map[string]any{"mode": "latest"}, "topics": []string{topic},
	})
	drainAndClose(seedResp)
	if seedResp.StatusCode != http.StatusOK {
		t.Fatalf("pre-seed status = %d, want 200", seedResp.StatusCode)
	}

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/consumer-groups/"+target+"/clone-offsets", operatorKey, map[string]any{
		"confirm": target, "source": source,
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Target  string `json:"target"`
		Offsets []struct {
			Topic  string `json:"topic"`
			Offset int64  `json:"offset"`
		} `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Target != target || len(body.Offsets) != 1 || body.Offsets[0].Offset != 7 {
		t.Fatalf("body = %+v, want target %q with one offset of 7", body, target)
	}
}
