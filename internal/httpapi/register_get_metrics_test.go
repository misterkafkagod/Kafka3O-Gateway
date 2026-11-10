package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2/humatest"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type fakeGetMetrics struct{ result metrics.ClusterMetrics }

func (f fakeGetMetrics) Execute(context.Context) metrics.ClusterMetrics { return f.result }

func TestRegisterGetMetrics_ReturnsSectionsWithUnknownSizesAsNull(t *testing.T) {
	total := int64(94521434112)
	_, api := humatest.New(t)
	RegisterGetMetrics(api, fakeGetMetrics{result: metrics.ClusterMetrics{
		ReadAt: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC),
		LogDirs: metrics.LogDirs{Dirs: []metrics.LogDir{
			{Broker: 1, Dir: "/var/lib/kafka/data", TotalBytes: &total, KafkaBytes: 203606},
		}},
	}})
	response := api.Get("/api/v1/metrics")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	dir := body["logDirs"].(map[string]any)["dirs"].([]any)[0].(map[string]any)
	if body["readAt"] != "2026-11-10T12:00:00Z" || dir["totalBytes"] != float64(total) || dir["usableBytes"] != nil || dir["kafkaBytes"] != float64(203606) {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestRegisterGetMetrics_ReportsAnUnavailableSection(t *testing.T) {
	_, api := humatest.New(t)
	RegisterGetMetrics(api, fakeGetMetrics{result: metrics.ClusterMetrics{LogDirs: metrics.LogDirs{Error: "describe log directories: CLUSTER_AUTHORIZATION_FAILED", Dirs: []metrics.LogDir{}}}})
	response := api.Get("/api/v1/metrics")
	var body struct {
		LogDirs struct {
			Error string `json:"error"`
			Dirs  []any  `json:"dirs"`
		} `json:"logDirs"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != http.StatusOK {
		t.Fatalf("status = %d, err = %v", response.Code, err)
	}
	if body.LogDirs.Error != "describe log directories: CLUSTER_AUTHORIZATION_FAILED" || body.LogDirs.Dirs == nil {
		t.Fatalf("body = %s", response.Body.String())
	}
}
