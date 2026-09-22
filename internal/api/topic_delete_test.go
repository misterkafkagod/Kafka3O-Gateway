package api_test

import (
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_T7_DeleteWithBodyConfirm(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-del", 1)

	resp := gw.Do(t, http.MethodDelete, "/v1/topics/t-del", map[string]any{"confirm": "t-del"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["deleted"] != "t-del" {
		t.Errorf("deleted = %v, want t-del", body["deleted"])
	}

	getResp := gw.Get(t, "/v1/topics/t-del")
	getBody := decodeBody(t, getResp)
	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /v1/topics/t-del after delete = %d, want 404: %v", getResp.StatusCode, getBody)
	}
}

func TestAPI_T8_DryRunReturnsTopicsAndToken(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("tmp-a", 1)
	gw.Fake.SeedTopic("tmp-b", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/batch/topics/delete?dryRun=true", map[string]any{"pattern": "^tmp-"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if body["dryRun"] != true {
		t.Errorf("dryRun = %v, want true", body["dryRun"])
	}
	plan, ok := body["plan"].(map[string]any)
	if !ok {
		t.Fatalf("plan = %v, want an object", body["plan"])
	}
	topics, _ := plan["topics"].([]any)
	if len(topics) != 2 || topics[0] != "tmp-a" || topics[1] != "tmp-b" {
		t.Fatalf("plan.topics = %v, want [tmp-a tmp-b]", plan["topics"])
	}
	token, _ := plan["planToken"].(string)
	if len(token) != 64 {
		t.Errorf("plan.planToken = %q, want 64 hex characters", token)
	}

	getResp := gw.Get(t, "/v1/topics/tmp-a")
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("GET /v1/topics/tmp-a after a dry-run bulk delete = %d, want 200 (nothing deleted)", getResp.StatusCode)
	}
}

func TestAPI_T8_StaleToken400WithDetailsPlan(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("tmp-a", 1)
	gw.Fake.SeedTopic("tmp-b", 1)

	planResp := gw.Do(t, http.MethodPost, "/v1/batch/topics/delete?dryRun=true", map[string]any{"pattern": "^tmp-"})
	planBody := decodeBody(t, planResp)
	plan, _ := planBody["plan"].(map[string]any)
	staleToken, _ := plan["planToken"].(string)
	if staleToken == "" {
		t.Fatalf("plan.planToken missing from dry-run response: %v", planBody)
	}

	gw.Fake.SeedTopic("tmp-c", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/batch/topics/delete", map[string]any{"confirm": staleToken, "pattern": "^tmp-"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "CONFIRMATION_MISMATCH" {
		t.Errorf("code = %v, want CONFIRMATION_MISMATCH", errBody["code"])
	}
	details, _ := errBody["details"].(map[string]any)
	freshPlan, _ := details["plan"].(map[string]any)
	freshTopics, _ := freshPlan["topics"].([]any)
	if len(freshTopics) != 3 {
		t.Errorf("details.plan.topics = %v, want three topics (tmp-a, tmp-b, tmp-c)", freshPlan["topics"])
	}
}

func TestAPI_T11_LowWatermarkReturned(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-recs", 1, seedRecords(10)...)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t-recs/delete-records", map[string]any{
		"confirm": "t-recs", "offsets": map[string]any{"0": 5},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	partitions, _ := body["partitions"].([]any)
	if len(partitions) != 1 {
		t.Fatalf("partitions = %v, want exactly one", partitions)
	}
	p, _ := partitions[0].(map[string]any)
	if p["id"] != 0.0 || p["lowWatermark"] != 5.0 {
		t.Errorf("partitions[0] = %v, want {id:0 lowWatermark:5}", p)
	}
}

func TestAPI_T12_PurgeThenReadEmpty(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-purge", 1, seedRecords(5)...)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t-purge/purge", map[string]any{"confirm": "t-purge"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}

	readResp := gw.Get(t, "/v1/topics/t-purge/messages?from=beginning")
	readBody := decodeBody(t, readResp)
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("GET messages after purge = %d, want 200: %v", readResp.StatusCode, readBody)
	}
	items, _ := readBody["items"].([]any)
	if len(items) != 0 {
		t.Errorf("items = %v, want none", items)
	}
	scan, _ := readBody["scan"].(map[string]any)
	if scan["reachedEnd"] != true {
		t.Errorf("scan.reachedEnd = %v, want true", scan["reachedEnd"])
	}
}

func TestAPI_T7T8T11T12_ResultSeverityHigh(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t-del", 1)
	gw.Fake.SeedTopic("tmp-a", 1)
	gw.Fake.SeedTopic("t-recs", 1, seedRecords(5)...)
	gw.Fake.SeedTopic("t-purge", 1, seedRecords(5)...)

	resp1 := gw.Do(t, http.MethodDelete, "/v1/topics/t-del", map[string]any{"confirm": "t-del"})
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("T7 status = %d, want 200", resp1.StatusCode)
	}
	_ = decodeBody(t, resp1)

	planResp := gw.Do(t, http.MethodPost, "/v1/batch/topics/delete?dryRun=true", map[string]any{"pattern": "^tmp-"})
	planBody := decodeBody(t, planResp)
	plan, _ := planBody["plan"].(map[string]any)
	token, _ := plan["planToken"].(string)
	if token == "" {
		t.Fatalf("T8 dry-run plan.planToken missing: %v", planBody)
	}
	resp2 := gw.Do(t, http.MethodPost, "/v1/batch/topics/delete", map[string]any{"confirm": token, "pattern": "^tmp-"})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("T8 status = %d, want 200", resp2.StatusCode)
	}
	_ = decodeBody(t, resp2)

	resp3 := gw.Do(t, http.MethodPost, "/v1/topics/t-recs/delete-records", map[string]any{
		"confirm": "t-recs", "offsets": map[string]any{"0": 5},
	})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("T11 status = %d, want 200", resp3.StatusCode)
	}
	_ = decodeBody(t, resp3)

	resp4 := gw.Do(t, http.MethodPost, "/v1/topics/t-purge/purge", map[string]any{"confirm": "t-purge"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("T12 status = %d, want 200", resp4.StatusCode)
	}
	_ = decodeBody(t, resp4)

	results := map[string]*audit.Event{}
	events := gw.Audit.Events()
	for i := range events {
		ev := events[i]
		if ev.Phase == audit.PhaseResult {
			results[ev.CommandID] = &events[i]
		}
	}
	for _, id := range []string{"T7", "T8", "T11", "T12"} {
		ev := results[id]
		if ev == nil || ev.Severity != audit.SeverityHigh {
			t.Errorf("%s RESULT = %+v, want Severity HIGH", id, ev)
		}
	}
}

// seedRecords builds n distinct partition-0 records for SeedTopic.
func seedRecords(n int) []kafka.Record {
	out := make([]kafka.Record, n)
	for i := range out {
		out[i] = kafka.Record{Partition: 0, Value: []byte{byte(i)}}
	}
	return out
}
