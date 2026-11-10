package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

func TestNewPrometheusHandler_ServesOnlyTheMetricsPath(t *testing.T) {
	handler := NewPrometheusHandler(fakeGetMetrics{result: metrics.ClusterMetrics{Partitions: metrics.Partitions{Topics: 3}}})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || !strings.Contains(response.Body.String(), "kafka3o_topics 3\n") {
		t.Fatalf("metrics = %d %q %s", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
	for path, want := range map[string]int{"/api/v1/cluster": http.StatusNotFound, "/": http.StatusNotFound, "/metrics/": http.StatusNotFound} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Errorf("%s = %d", path, response.Code)
		}
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if response.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST = %d", response.Code)
	}
}
