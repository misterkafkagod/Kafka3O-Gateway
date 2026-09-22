//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_M3_SearchRegex(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/messages/search", operatorKey, map[string]any{
		"regex": ".", "from": "beginning", "maxMatches": 10,
	})
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Items []struct {
			Topic string `json:"topic"`
		} `json:"items"`
		Scan struct {
			Scanned int `json:"scanned"`
			Matched int `json:"matched"`
		} `json:"scan"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Scan.Scanned < body.Scan.Matched {
		t.Errorf("scan = %+v, want scanned >= matched", body.Scan)
	}
}

func TestAcceptance_M4_FilterJSONPath(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptanceJSON(t, http.MethodPost, "/v1/topics/"+topic+"/messages/filter", operatorKey, map[string]any{
		"filter": map[string]any{"path": "$", "op": "exists"}, "from": "beginning", "maxMatches": 10,
	})
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Scan struct {
			Scanned int `json:"scanned"`
			Skipped int `json:"skipped"`
		} `json:"scan"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Scan.Scanned < 0 || body.Scan.Skipped < 0 {
		t.Errorf("scan = %+v, want non-negative counts", body.Scan)
	}
}
