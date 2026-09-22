//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_M8_Replay(t *testing.T) {
	t.Parallel()
	src := "acc-" + runID + "-m8-src"
	dst := "acc-" + runID + "-m8-dst"

	for _, name := range []string{src, dst} {
		resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics", operatorKey, map[string]any{
			"name": name, "partitions": 1, "replicationFactor": 1,
		})
		drainAndClose(resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %s status = %d, want 201", name, resp.StatusCode)
		}
	}

	for range 5 {
		r := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+src+"/messages", operatorKey, map[string]any{
			"records": []map[string]any{{"value": "v"}},
		})
		drainAndClose(r)
		if r.StatusCode != http.StatusOK {
			t.Fatalf("produce to %s status = %d, want 200", src, r.StatusCode)
		}
	}

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/replays", operatorKey, map[string]any{
		"confirm": dst,
		"source":  map[string]any{"topic": src, "from": "beginning"},
		"target":  map[string]any{"topic": dst},
		"limit":   1000,
	})
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Copied     int              `json:"copied"`
		ReachedEnd bool             `json:"reachedEnd"`
		Cursor     map[string]int64 `json:"cursor"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Copied != 5 || !body.ReachedEnd {
		t.Fatalf("body = %+v, want copied 5, reachedEnd true", body)
	}

	describeResp := doAcceptance(t, http.MethodGet, "/v1/topics/"+dst, operatorKey)
	defer drainAndClose(describeResp)
	var describeBody struct {
		ApproxMessageCount int64 `json:"approxMessageCount"`
	}
	if err := json.NewDecoder(describeResp.Body).Decode(&describeBody); err != nil {
		t.Fatalf("decode describe response: %v", err)
	}
	if describeBody.ApproxMessageCount != 5 {
		t.Errorf("dst approxMessageCount = %d, want 5", describeBody.ApproxMessageCount)
	}
}
