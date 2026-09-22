package api_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/testutil"
)

func TestAPI_M8_PreservePartitionMismatch400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("src", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 1, Value: []byte("b")},
	)
	gw.Fake.SeedTopic("dst", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/replays", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "beginning"},
		"target":  map[string]any{"topic": "dst", "preservePartition": true},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "PARTITION_MISMATCH" {
		t.Errorf("code = %v, want PARTITION_MISMATCH", errBody["code"])
	}
}

func TestAPI_M8_MidBatchFailure502WithProgress(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
	)
	gw.Fake.SeedTopic("dst", 1)

	first := gw.Do(t, http.MethodPost, "/v1/replays", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "beginning"},
		"target":  map[string]any{"topic": "dst"},
		"limit":   1,
	})
	firstBody := decodeBody(t, first)
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200: %v", first.StatusCode, firstBody)
	}
	cursor, _ := firstBody["cursor"].(map[string]any)
	next, _ := cursor["0"].(float64)

	gw.Fake.FailNext("Produce", kafka.KindBroker)
	resp := gw.Do(t, http.MethodPost, "/v1/replays", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "offset:" + strconv.Itoa(int(next))},
		"target":  map[string]any{"topic": "dst"},
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "KAFKA_ERROR" {
		t.Errorf("code = %v, want KAFKA_ERROR", errBody["code"])
	}
	details, _ := errBody["details"].(map[string]any)
	progress, _ := details["progress"].(map[string]any)
	if progress["copied"] != 0.0 {
		t.Errorf("details.progress.copied = %v, want 0", progress["copied"])
	}
	progressCursor, _ := progress["cursor"].(map[string]any)
	if progressCursor["0"] != next {
		t.Errorf("details.progress.cursor = %v, want {0: %v}", progressCursor, next)
	}
}

func TestAPI_M8_CursorResume(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	gw.Fake.SeedTopic("dst", 1)

	first := gw.Do(t, http.MethodPost, "/v1/replays", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "beginning"},
		"target":  map[string]any{"topic": "dst"},
		"limit":   2,
	})
	firstBody := decodeBody(t, first)
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200: %v", first.StatusCode, firstBody)
	}
	if firstBody["copied"] != 2.0 || firstBody["reachedEnd"] != false {
		t.Fatalf("first = %v, want copied 2, reachedEnd false", firstBody)
	}
	cursor, _ := firstBody["cursor"].(map[string]any)
	next, _ := cursor["0"].(float64)

	second := gw.Do(t, http.MethodPost, "/v1/replays", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "offset:" + strconv.Itoa(int(next))},
		"target":  map[string]any{"topic": "dst"},
	})
	secondBody := decodeBody(t, second)
	if second.StatusCode != http.StatusOK {
		t.Fatalf("second status = %d, want 200: %v", second.StatusCode, secondBody)
	}
	if secondBody["copied"] != 1.0 || secondBody["reachedEnd"] != true {
		t.Fatalf("second = %v, want copied 1, reachedEnd true", secondBody)
	}
}

func TestAPI_M8_LimitAboveCeiling400(t *testing.T) {
	t.Parallel()
	gw := testutil.NewTestGateway(t)
	gw.Fake.SeedTopic("src", 1)
	gw.Fake.SeedTopic("dst", 1)

	resp := gw.Do(t, http.MethodPost, "/v1/replays", map[string]any{
		"confirm": "dst",
		"source":  map[string]any{"topic": "src", "from": "beginning"},
		"target":  map[string]any{"topic": "dst"},
		"limit":   999999,
	})
	body := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %v", resp.StatusCode, body)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "BOUND_EXCEEDED" {
		t.Errorf("code = %v, want BOUND_EXCEEDED", errBody["code"])
	}
}
