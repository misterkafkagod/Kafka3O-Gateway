package httpapi

import (
	"bytes"
	"net/http"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type prometheusHandler struct {
	getMetrics metrics.GetMetrics
}

// NewPrometheusHandler serves only GET /metrics in the Prometheus text format; it never reaches the Kafka API routes.
func NewPrometheusHandler(getMetrics metrics.GetMetrics) http.Handler {
	return prometheusHandler{getMetrics: getMetrics}
}

func (h prometheusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body bytes.Buffer
	if err := metrics.WritePrometheusText(&body, h.getMetrics.Execute(r.Context())); err != nil {
		http.Error(w, "could not write metrics", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write(body.Bytes())
}
