package health

import (
	"context"
	"net/http"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// ClusterProbe is the narrow surface Ready needs from the Kafka port
// (FUNC-SPEC §8.7 C3): describing the cluster to report reachability.
type ClusterProbe interface {
	DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error)
}

// AuditStatus reports the configured audit sink and whether it is currently
// healthy (TECH-SPEC §6.1 B5). internal/api may not import internal/audit
// (TECH-SPEC §5.3), so the composition root supplies this function once the
// audit sink exists (Phase 4) — readiness never gates on its result.
type AuditStatus func() (sink string, healthy bool)

// ReadyCluster is the cluster sub-object of /health/ready (FUNC-SPEC §8.7 C3).
type ReadyCluster struct {
	Reachable   bool  `json:"reachable"`
	BrokersSeen int   `json:"brokersSeen"`
	LatencyMs   int64 `json:"latencyMs"`
}

// ReadyAudit is the audit sub-object of /health/ready (TECH-SPEC §6.1 B5):
// reported for visibility, never gates readiness.
type ReadyAudit struct {
	Sink    string `json:"sink"`
	Healthy bool   `json:"healthy"`
}

// ReadyBody is the /health/ready response body.
type ReadyBody struct {
	Status  string       `json:"status"`
	Cluster ReadyCluster `json:"cluster"`
	Audit   ReadyAudit   `json:"audit"`
}

// ReadyInput is empty: readiness takes no parameters.
type ReadyInput struct{}

// ReadyOutput wraps ReadyBody. Status is Huma's dynamic-status-code field
// (matched by name, not tag): 200 when the cluster is reachable, 503 when
// it is DOWN (FUNC-SPEC O6).
type ReadyOutput struct {
	Status int
	Body   ReadyBody
}

// Ready builds the /health/ready handler for a cluster probe and an
// audit-status function (FUNC-SPEC §8.7 C3, O6; TECH-SPEC §6.1 B5).
func Ready(probe ClusterProbe, audit AuditStatus) func(context.Context, *ReadyInput) (*ReadyOutput, error) {
	return func(ctx context.Context, _ *ReadyInput) (*ReadyOutput, error) {
		start := time.Now()
		info, err := probe.DescribeCluster(ctx)
		latencyMs := time.Since(start).Milliseconds()

		status, code := "UP", http.StatusOK
		if err != nil {
			status, code = "DOWN", http.StatusServiceUnavailable
		}

		sink, healthy := audit()

		return &ReadyOutput{
			Status: code,
			Body: ReadyBody{
				Status: status,
				Cluster: ReadyCluster{
					Reachable:   err == nil,
					BrokersSeen: len(info.Brokers),
					LatencyMs:   latencyMs,
				},
				Audit: ReadyAudit{Sink: sink, Healthy: healthy},
			},
		}, nil
	}
}
