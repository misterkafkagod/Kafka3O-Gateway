//go:build acceptance

package acceptance

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
)

// acceptanceGroupOrSkip returns the pre-existing consumer group id
// KGW_ACC_GROUP names, skipping t if it is unset — G1-G3 need a group with
// committed offsets that already exists on the cluster (the manual test
// plan's own precondition: run a console consumer in this group on
// KGW_ACC_TOPIC, then stop it).
func acceptanceGroupOrSkip(t *testing.T) string {
	t.Helper()
	group := os.Getenv("KGW_ACC_GROUP")
	if group == "" {
		t.Skip("KGW_ACC_GROUP is unset; skipping a test that needs a pre-existing consumer group")
	}
	return group
}

func TestAcceptance_G1_ListGroups(t *testing.T) {
	t.Parallel()
	group := acceptanceGroupOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/consumer-groups", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Items []struct {
			GroupID string `json:"groupId"`
			State   string `json:"state"`
		} `json:"items"`
		Page struct {
			Total int `json:"total"`
		} `json:"page"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	found := false
	for _, item := range body.Items {
		if item.GroupID == group {
			found = true
		}
	}
	if !found {
		t.Errorf("items = %+v, want KGW_ACC_GROUP (%s) present", body.Items, group)
	}
}

func TestAcceptance_G2_DescribeGroup(t *testing.T) {
	t.Parallel()
	group := acceptanceGroupOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/consumer-groups/"+group, operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		GroupID  string `json:"groupId"`
		TotalLag int64  `json:"totalLag"`
		Offsets  []struct {
			Topic     string `json:"topic"`
			Committed int64  `json:"committed"`
			End       int64  `json:"end"`
			Lag       int64  `json:"lag"`
		} `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.GroupID != group {
		t.Errorf("groupId = %q, want %q", body.GroupID, group)
	}
	if len(body.Offsets) == 0 {
		t.Error("offsets = [], want at least one committed offset (KGW_ACC_GROUP must have consumed something)")
	}
}

func TestAcceptance_G3_TopicConsumers(t *testing.T) {
	t.Parallel()
	topic := acceptanceTopicOrSkip(t)
	group := acceptanceGroupOrSkip(t)

	resp := doAcceptance(t, http.MethodGet, "/v1/topics/"+topic+"/consumer-groups", operatorKey)
	defer drainAndClose(resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Topic  string `json:"topic"`
		Groups []struct {
			GroupID string `json:"groupId"`
		} `json:"groups"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Topic != topic {
		t.Errorf("topic = %q, want %q", body.Topic, topic)
	}
	found := false
	for _, g := range body.Groups {
		if g.GroupID == group {
			found = true
		}
	}
	if !found {
		t.Errorf("groups = %+v, want KGW_ACC_GROUP (%s) present (it must have committed offsets on KGW_ACC_TOPIC)", body.Groups, group)
	}
}
