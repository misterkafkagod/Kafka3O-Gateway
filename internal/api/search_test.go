package api_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_M3_InvalidRegex400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/search", map[string]any{"regex": "(", "from": "beginning"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "INVALID_REGEX" {
		t.Errorf("code = %v, want INVALID_REGEX", errBody["code"])
	}
}

func TestAPI_M4_InvalidJSONPath400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte(`{"a":1}`)})

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/filter", map[string]any{
		"filter": map[string]any{"path": "not a valid path", "op": "eq", "value": "x"},
		"from":   "beginning",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "INVALID_JSONPATH" {
		t.Errorf("code = %v, want INVALID_JSONPATH", errBody["code"])
	}
}

func TestAPI_M3M4_LimitRejected400ValidationFailed(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	searchResp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/search", map[string]any{"regex": "a", "from": "beginning", "limit": 5})
	defer func() { _ = searchResp.Body.Close() }()
	if searchResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("search status = %d, want 400", searchResp.StatusCode)
	}
	var searchBody map[string]any
	_ = json.NewDecoder(searchResp.Body).Decode(&searchBody)
	if errBody, _ := searchBody["error"].(map[string]any); errBody["code"] != "VALIDATION_FAILED" {
		t.Errorf("search code = %v, want VALIDATION_FAILED", errBody["code"])
	}

	filterResp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/filter", map[string]any{
		"filter": map[string]any{"path": "$.a", "op": "exists"}, "from": "beginning", "limit": 5,
	})
	defer func() { _ = filterResp.Body.Close() }()
	if filterResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("filter status = %d, want 400", filterResp.StatusCode)
	}
	var filterBody map[string]any
	_ = json.NewDecoder(filterResp.Body).Decode(&filterBody)
	if errBody, _ := filterBody["error"].(map[string]any); errBody["code"] != "VALIDATION_FAILED" {
		t.Errorf("filter code = %v, want VALIDATION_FAILED", errBody["code"])
	}
}

func TestAPI_M3_MatchesAndScanStats(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("FAILED: disk full")},
		kafka.Record{Partition: 0, Value: []byte("OK: all good")},
		kafka.Record{Partition: 0, Value: []byte("FAILED: timeout")},
	)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/search", map[string]any{"regex": "FAILED", "from": "beginning"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	items, _ := body["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	scanBody, _ := body["scan"].(map[string]any)
	if scanBody["scanned"] != float64(3) || scanBody["matched"] != float64(2) {
		t.Errorf("scan = %v, want scanned=3 matched=2 (scanned >= matched)", scanBody)
	}
}

func TestAPI_M4_EachOpThroughHTTP(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		json  string
		op    string
		value any
		want  bool
	}{
		{"eq", `{"v":"FAILED"}`, "eq", "FAILED", true},
		{"neq", `{"v":"OK"}`, "neq", "FAILED", true},
		{"contains", `{"v":"a FAILED b"}`, "contains", "FAILED", true},
		{"regex", `{"v":"FAILED-123"}`, "regex", "^FAILED", true},
		{"exists", `{"v":"anything"}`, "exists", nil, true},
		{"gt", `{"v":5}`, "gt", float64(3), true},
		{"lt", `{"v":1}`, "lt", float64(3), true},
		{"gte", `{"v":3}`, "gte", float64(3), true},
		{"lte", `{"v":3}`, "lte", float64(3), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gw := testutil.NewTestGateway(t)
			gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte(tc.json)})

			body := map[string]any{"from": "beginning", "filter": map[string]any{"path": "$.v", "op": tc.op}}
			if tc.value != nil {
				body["filter"].(map[string]any)["value"] = tc.value
			}
			resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/filter", body)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			var respBody map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&respBody)
			items, _ := respBody["items"].([]any)
			got := len(items) > 0
			if got != tc.want {
				t.Errorf("op %s matched=%v, want %v (items=%v)", tc.op, got, tc.want, items)
			}
		})
	}
}

func TestAPI_M4_SkippedReported(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte(`{"status":"FAILED"}`)},
		kafka.Record{Partition: 0, Value: []byte("not json")},
	)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages/filter", map[string]any{
		"filter": map[string]any{"path": "$.status", "op": "eq", "value": "FAILED"},
		"from":   "beginning",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	scanBody, _ := body["scan"].(map[string]any)
	if scanBody["skipped"] != float64(1) {
		t.Errorf("scan.skipped = %v, want 1", scanBody["skipped"])
	}
}

func TestAPI_M3M4_ReaderAllowed(t *testing.T) {
	t.Parallel()
	gw := readerGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte(`{"v":"FAILED"}`)})

	searchResp := gw.DoWithKey(t, http.MethodPost, "/v1/topics/t/messages/search", map[string]any{"regex": "FAILED", "from": "beginning"}, readerSecret)
	defer func() { _ = searchResp.Body.Close() }()
	if searchResp.StatusCode != http.StatusOK {
		t.Errorf("search with reader key = %d, want 200", searchResp.StatusCode)
	}

	filterResp := gw.DoWithKey(t, http.MethodPost, "/v1/topics/t/messages/filter", map[string]any{
		"filter": map[string]any{"path": "$.v", "op": "eq", "value": "FAILED"}, "from": "beginning",
	}, readerSecret)
	defer func() { _ = filterResp.Body.Close() }()
	if filterResp.StatusCode != http.StatusOK {
		t.Errorf("filter with reader key = %d, want 200", filterResp.StatusCode)
	}
}
