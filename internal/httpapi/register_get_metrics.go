package httpapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type GetMetricsOutput struct {
	Body metrics.ClusterMetrics
}

// RegisterGetMetrics registers GET /api/v1/metrics: what Kafka reports about the cluster's disks;
// a section Kafka cannot answer carries its error instead of values, so the route itself always answers.
func RegisterGetMetrics(api huma.API, getMetrics metrics.GetMetrics) {
	huma.Register(api, huma.Operation{
		OperationID: "get-metrics",
		Method:      http.MethodGet,
		Path:        "/api/v1/metrics",
		Summary:     "Cluster metrics reported by Kafka",
	}, func(ctx context.Context, _ *struct{}) (*GetMetricsOutput, error) {
		return &GetMetricsOutput{Body: getMetrics.Execute(ctx)}, nil
	})
}
