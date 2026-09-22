package api_test

import (
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_G4_409Envelope(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-demo", 1)
	gw.Fake.SeedGroup("g1", nil)
	gw.Fake.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m1"})

	resp := gw.Do(t, http.MethodPost, "/v1/consumer-groups/g1/reset-offsets", map[string]any{
		"confirm": "g1", "target": map[string]any{"mode": "earliest"}, "topics": []string{"t-demo"},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "GROUP_ACTIVE" {
		t.Errorf("code = %v, want GROUP_ACTIVE", errBody["code"])
	}
}

func TestAPI_G5_ResultSeverityHigh(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedGroup("g1", nil)

	resp := gw.Do(t, http.MethodDelete, "/v1/consumer-groups/g1", map[string]any{"confirm": "g1"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["deleted"] != "g1" {
		t.Errorf("deleted = %v, want g1", body["deleted"])
	}

	events := gw.Audit.Events()
	var result *audit.Event
	for i := range events {
		if events[i].Phase == audit.PhaseResult && events[i].CommandID == "G5" {
			result = &events[i]
		}
	}
	if result == nil || result.Severity != audit.SeverityHigh {
		t.Fatalf("G5 RESULT = %+v, want Severity HIGH", result)
	}
}

func TestAPI_G6_RemovedList(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedGroup("g1", nil)
	gw.Fake.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m1"})
	gw.Fake.SeedGroupMember("g1", kafka.GroupMember{MemberID: "m2"})

	resp := gw.Do(t, http.MethodPost, "/v1/consumer-groups/g1/remove-members", map[string]any{"confirm": "g1"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	removed, _ := body["removed"].([]any)
	if len(removed) != 2 {
		t.Fatalf("removed = %v, want both members", removed)
	}
}

func TestAPI_G7_TargetInPathConfirmIsTarget(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedGroup("g-source", map[kafka.TopicPartition]int64{{Topic: "t-demo", Partition: 0}: 5})

	// confirm must equal the *target* group id (the one in the path), never the source.
	wrong := gw.Do(t, http.MethodPost, "/v1/consumer-groups/g-target/clone-offsets", map[string]any{
		"confirm": "g-source", "source": "g-source",
	})
	wrongBody := decodeBody(t, wrong)
	if wrong.StatusCode != http.StatusBadRequest {
		t.Fatalf("confirm=source status = %d, want 400: %v", wrong.StatusCode, wrongBody)
	}

	resp := gw.Do(t, http.MethodPost, "/v1/consumer-groups/g-target/clone-offsets", map[string]any{
		"confirm": "g-target", "source": "g-source",
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("confirm=target status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["target"] != "g-target" {
		t.Errorf("target = %v, want g-target", body["target"])
	}
}
