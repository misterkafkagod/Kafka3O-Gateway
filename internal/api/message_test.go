package api_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_M1_LimitAboveCeiling400BoundExceeded(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	resp := gw.Get(t, "/v1/topics/t/messages?from=beginning&limit=999999")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "BOUND_EXCEEDED" {
		t.Errorf("code = %v, want BOUND_EXCEEDED", errBody["code"])
	}
}

func TestAPI_M1_StoppedByMaxMessagesWithContinuation(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)

	resp := gw.Get(t, "/v1/topics/t/messages?from=beginning&limit=2")
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
	if scanBody["stoppedBy"] != "maxMessages" {
		t.Errorf("scan.stoppedBy = %v, want maxMessages", scanBody["stoppedBy"])
	}
	continuation, _ := scanBody["continuation"].(map[string]any)
	if continuation["0"] != float64(2) {
		t.Errorf("scan.continuation[0] = %v, want 2", continuation["0"])
	}
}

func TestAPI_M1_StoppedByMaxBytes(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("aaaaa")},
		kafka.Record{Partition: 0, Value: []byte("bbbbb")},
		kafka.Record{Partition: 0, Value: []byte("ccccc")},
	)

	resp := gw.Get(t, "/v1/topics/t/messages?from=beginning&maxBytes=8")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	scanBody, _ := body["scan"].(map[string]any)
	if scanBody["stoppedBy"] != "maxBytes" {
		t.Errorf("scan.stoppedBy = %v, want maxBytes", scanBody["stoppedBy"])
	}
}

func TestAPI_M1_StoppedByMaxTime(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	gw.Fake.Latency("Poll", 200*time.Millisecond)

	resp := gw.Get(t, "/v1/topics/t/messages?from=beginning&maxTimeMs=20")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	scanBody, _ := body["scan"].(map[string]any)
	if scanBody["stoppedBy"] != "maxTime" {
		t.Errorf("scan.stoppedBy = %v, want maxTime", scanBody["stoppedBy"])
	}
}

func TestAPI_M1_LatestOrdering(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	gw.Fake.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("0"), Timestamp: base},
		kafka.Record{Partition: 0, Value: []byte("1"), Timestamp: base.Add(1 * time.Second)},
		kafka.Record{Partition: 0, Value: []byte("2"), Timestamp: base.Add(2 * time.Second)},
	)

	resp := gw.Get(t, "/v1/topics/t/messages?from=latest&limit=2")
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
	first, _ := items[0].(map[string]any)
	second, _ := items[1].(map[string]any)
	if first["value"] != "2" || second["value"] != "1" {
		t.Errorf("items = [%v, %v], want [2, 1] (descending timestamp)", first["value"], second["value"])
	}
	scanBody, _ := body["scan"].(map[string]any)
	if scanBody["reachedEnd"] != true {
		t.Errorf("scan.reachedEnd = %v, want true", scanBody["reachedEnd"])
	}
}

func TestAPI_M1_FormatBase64(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte(`{"a":1}`)})

	resp := gw.Get(t, "/v1/topics/t/messages?from=beginning&format=base64")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	items, _ := body["items"].([]any)
	item, _ := items[0].(map[string]any)
	if item["valueEncoding"] != "base64" {
		t.Fatalf("valueEncoding = %v, want base64", item["valueEncoding"])
	}
	if want := base64.StdEncoding.EncodeToString([]byte(`{"a":1}`)); item["value"] != want {
		t.Errorf("value = %v, want %v", item["value"], want)
	}
}

func TestAPI_M2_ReturnsRecord(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)

	resp := gw.Get(t, "/v1/topics/t/partitions/0/messages/2")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["value"] != "c" || body["offset"] != float64(2) {
		t.Errorf("body = %v, want offset 2 value c", body)
	}
}

func TestAPI_M1_InvalidFrom400ValidationFailed(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	resp := gw.Get(t, "/v1/topics/t/messages?from=whenever")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "VALIDATION_FAILED" {
		t.Errorf("code = %v, want VALIDATION_FAILED", errBody["code"])
	}
}

func TestAPI_M2_404Envelope(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})

	resp := gw.Get(t, "/v1/topics/t/partitions/0/messages/999999")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "NOT_FOUND" {
		t.Errorf("code = %v, want NOT_FOUND", errBody["code"])
	}
}
