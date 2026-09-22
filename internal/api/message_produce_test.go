package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

// doRaw issues a request with an arbitrary body and Content-Type, for M6's
// NDJSON/JSON-array/unsupported-type cases that gw.Do's JSON-only helper
// cannot express.
func doRaw(t *testing.T, gw *testutil.Gateway, method, path, contentType string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, gw.Server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequest(%s %q) error: %v", method, path, err)
	}
	if contentType != "" {
		req.Header.Set(middleware.HeaderContentType, contentType)
	}
	req.Header.Set(middleware.HeaderAPIKey, testutil.DefaultOperatorKey)
	resp, err := gw.Server.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s error: %v", method, path, err)
	}
	return resp
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return body
}

func TestAPI_M5M6M7_ReaderIs403TierForbidden(t *testing.T) {
	t.Parallel()
	gw := readerGateway(t)
	gw.Fake.SeedTopic("t", 1)

	routes := []struct {
		method, path string
		body         map[string]any
	}{
		{http.MethodPost, "/v1/topics/t/messages", map[string]any{"value": "v", "key": "k"}},
		{http.MethodPost, "/v1/topics/t/messages/bulk", map[string]any{"value": "v", "key": "k"}},
		{http.MethodPost, "/v1/topics/t/tombstones", map[string]any{"key": "k"}},
	}
	for _, r := range routes {
		resp := gw.DoWithKey(t, r.method, r.path, r.body, readerSecret)
		body := decodeBody(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s with reader key = %d, want 403", r.method, r.path, resp.StatusCode)
		}
		errBody, _ := body["error"].(map[string]any)
		if errBody["code"] != "TIER_FORBIDDEN" {
			t.Errorf("%s %s code = %v, want TIER_FORBIDDEN", r.method, r.path, errBody["code"])
		}
	}
}

func TestAPI_M5M6M7_NoKeyIs401(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)

	routes := []string{
		"/v1/topics/t/messages",
		"/v1/topics/t/messages/bulk",
		"/v1/topics/t/tombstones",
	}
	for _, path := range routes {
		resp := gw.DoWithKey(t, http.MethodPost, path, map[string]any{"value": "v"}, "")
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("POST %s with no key = %d, want 401", path, resp.StatusCode)
		}
	}
}

func TestAPI_M5_AttemptBeforeProduceBeforeResult(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages", map[string]any{"value": "hello"})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}

	events := gw.Audit.Events()
	if len(events) != 2 || events[0].Phase != audit.PhaseAttempt || events[1].Phase != audit.PhaseResult {
		t.Fatalf("audit events = %+v, want [ATTEMPT, RESULT]", events)
	}
	if events[1].Outcome != audit.OutcomeSucceeded {
		t.Errorf("RESULT.Outcome = %s, want SUCCEEDED", events[1].Outcome)
	}
	gw.Fake.AssertCalled(t, "Produce")
}

func TestAPI_M5_AuditAttemptFailure503NoMutation(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)
	gw.Audit.FailNext(audit.PhaseAttempt)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages", map[string]any{"value": "hello"})
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "AUDIT_UNAVAILABLE" {
		t.Errorf("code = %v, want AUDIT_UNAVAILABLE", errBody["code"])
	}
	if calls := gw.Fake.MutatingCalls(); len(calls) != 0 {
		t.Errorf("MutatingCalls() = %+v, want none", calls)
	}
}

func TestAPI_M5_BulkValidationFailed400WithItems(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages", map[string]any{
		"records": []map[string]any{{"value": "ok"}, {"value": "bad", "partition": 99}},
	})
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "BULK_VALIDATION_FAILED" {
		t.Errorf("code = %v, want BULK_VALIDATION_FAILED", errBody["code"])
	}
	details, _ := errBody["details"].(map[string]any)
	items, _ := details["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("details.items = %v, want exactly the one failing item", details["items"])
	}
	if len(gw.Audit.Events()) != 0 {
		t.Errorf("audit events = %+v, want none (no ATTEMPT on validation failure)", gw.Audit.Events())
	}
}

func TestAPI_M5_PartialExecutionFailure207(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)
	gw.Fake.FailNext("Produce", kafka.KindBroker)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/messages", map[string]any{
		"records": []map[string]any{{"value": "a"}, {"value": "b"}},
	})
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207: %v", resp.StatusCode, body)
	}
	summary, _ := body["summary"].(map[string]any)
	if summary["total"] != 2.0 || summary["ok"] != 1.0 || summary["failed"] != 1.0 {
		t.Errorf("summary = %v, want {total:2 ok:1 failed:1}", summary)
	}
}

func TestAPI_M6_NDJSONAccepted(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)

	resp := doRaw(t, gw, http.MethodPost, "/v1/topics/t/messages/bulk", "application/x-ndjson",
		[]byte("{\"value\":\"a\"}\n{\"value\":\"b\"}\n"))
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	summary, _ := body["summary"].(map[string]any)
	if summary["total"] != 2.0 || summary["ok"] != 2.0 {
		t.Errorf("summary = %v, want {total:2 ok:2}", summary)
	}
}

func TestAPI_M6_OversizedBody413(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t, testutil.WithMaxBulkBodyBytes(16))
	gw.Fake.SeedTopic("t", 1)

	oversized := []byte(`[{"value":"this body is well over sixteen bytes"}]`)
	resp := doRaw(t, gw, http.MethodPost, "/v1/topics/t/messages/bulk", "application/json", oversized)
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "PAYLOAD_TOO_LARGE" {
		t.Errorf("code = %v, want PAYLOAD_TOO_LARGE", errBody["code"])
	}
}

func TestAPI_M6_UnsupportedMediaType415(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)

	resp := doRaw(t, gw, http.MethodPost, "/v1/topics/t/messages/bulk", "text/plain", []byte("nope"))
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "UNSUPPORTED_MEDIA_TYPE" {
		t.Errorf("code = %v, want UNSUPPORTED_MEDIA_TYPE", errBody["code"])
	}
}

func TestAPI_M7_TombstoneOffsetReturned(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/topics/t/tombstones", map[string]any{"key": "k1"})
	body := decodeBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", resp.StatusCode, body)
	}
	if _, ok := body["offset"]; !ok {
		t.Errorf("body = %v, want an offset field", body)
	}
	if _, ok := body["partition"]; !ok {
		t.Errorf("body = %v, want a partition field", body)
	}
}
