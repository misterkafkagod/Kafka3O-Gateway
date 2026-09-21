//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestAcceptance_C1_DescribeCluster(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		ClusterID string `json:"clusterId"`
		Brokers   []struct {
			ID int32 `json:"id"`
		} `json:"brokers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Brokers) == 0 {
		t.Error("brokers = [], want at least one (a dedicated acceptance cluster must be reachable)")
	}
}

func TestAcceptance_C2_BrokerConfig(t *testing.T) {
	t.Parallel()
	brokerID := firstBrokerID(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/brokers/"+strconv.Itoa(int(brokerID))+"/config", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Configs []struct {
			Name string `json:"name"`
		} `json:"configs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Configs) == 0 {
		t.Error("configs = [], want at least one broker configuration property")
	}
}

func TestAcceptance_C4_HealthSummary(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster/health", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Brokers struct {
			Online int `json:"online"`
		} `json:"brokers"`
		Partitions struct {
			Total int `json:"total"`
		} `json:"partitions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Brokers.Online == 0 {
		t.Error("brokers.online = 0, want at least one")
	}
}

func TestAcceptance_T1_ListTopics(t *testing.T) {
	t.Parallel()
	resp := doAcceptance(t, http.MethodGet, "/v1/topics?page=1&pageSize=50", readerKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
		Page struct {
			Total int `json:"total"`
		} `json:"page"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) > body.Page.Total {
		t.Errorf("items has %d entries but page.total = %d", len(body.Items), body.Page.Total)
	}
}

func TestAcceptance_T2_DescribeTopic(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic, operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Name       string `json:"name"`
		Partitions []struct {
			ID int32 `json:"id"`
		} `json:"partitions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Name != topic {
		t.Errorf("name = %q, want %q", body.Name, topic)
	}
	if len(body.Partitions) == 0 {
		t.Error("partitions = [], want at least one")
	}
}

func TestAcceptance_T3_TopicSize(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic+"/size", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		TotalBytes int64 `json:"totalBytes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.TotalBytes < 0 {
		t.Errorf("totalBytes = %d, want >= 0", body.TotalBytes)
	}
}

func TestAcceptance_T4_CountInWindow(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour).Format(time.RFC3339)
	to := now.Format(time.RFC3339)

	resp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic+"/count?from="+from+"&to="+to, operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Total int64 `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Total < 0 {
		t.Errorf("total = %d, want >= 0", body.Total)
	}
}

// acceptanceTopicOrSkip returns the pre-existing topic KGW_ACC_TOPIC names,
// skipping the test when it is unset. T2-T4 need a real topic with data on
// the target cluster, and depguard's acceptance rule bars this package from
// creating one itself (it may import internal/kafka but not franz or kadm) —
// creating it is out of scope by the same decision as CS1-CS3 in TASKS.md's
// Common Setup: environment provisioning is left to the human running this
// suite, e.g. via the Phase 2 Manual Test Plan's `t-demo` topic.
func acceptanceTopicOrSkip(t *testing.T) string {
	t.Helper()
	topic := os.Getenv("KGW_ACC_TOPIC")
	if topic == "" {
		t.Skip("KGW_ACC_TOPIC is unset; skipping a test that needs a pre-existing topic")
	}
	return topic
}

// firstBrokerID discovers a real broker id from the running cluster via C1,
// so C2's test does not have to guess one.
func firstBrokerID(t *testing.T) int32 {
	t.Helper()
	resp := doAcceptance(t, http.MethodGet, "/v1/cluster", operatorKey)
	defer drainAndClose(resp)

	var body struct {
		Brokers []struct {
			ID int32 `json:"id"`
		} `json:"brokers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /v1/cluster response: %v", err)
	}
	if len(body.Brokers) == 0 {
		t.Fatal("/v1/cluster returned no brokers")
	}
	return body.Brokers[0].ID
}
