//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAcceptance_C3_HealthLive(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/health/live", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "UP" {
		t.Errorf("status = %q, want UP", body.Status)
	}
}

func TestAcceptance_C3_HealthReady(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/health/ready", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a dedicated acceptance cluster must be reachable)", resp.StatusCode)
	}

	var body struct {
		Status  string `json:"status"`
		Cluster struct {
			Reachable   bool  `json:"reachable"`
			BrokersSeen int   `json:"brokersSeen"`
			LatencyMs   int64 `json:"latencyMs"`
		} `json:"cluster"`
		Audit struct {
			Sink    string `json:"sink"`
			Healthy bool   `json:"healthy"`
		} `json:"audit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if body.Status != "UP" {
		t.Errorf("status = %q, want UP", body.Status)
	}
	if !body.Cluster.Reachable {
		t.Error("cluster.reachable = false, want true")
	}
	if body.Cluster.BrokersSeen == 0 {
		t.Error("cluster.brokersSeen = 0, want at least one broker")
	}
}
