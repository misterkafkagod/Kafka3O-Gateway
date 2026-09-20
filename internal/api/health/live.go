// Package health implements the gateway's liveness and readiness probes
// (FUNC-SPEC §8.7 C3, O6; TECH-SPEC §6.1 B5). Both are read-only and
// ungated: they run before any command dispatch or tier check.
package health

import "context"

// LiveBody is the /health/live response body (FUNC-SPEC §8.7 C3).
type LiveBody struct {
	Status string `json:"status"`
}

// LiveInput is empty: liveness takes no parameters.
type LiveInput struct{}

// LiveOutput wraps LiveBody for Huma.
type LiveOutput struct {
	Body LiveBody
}

// Live always reports UP without touching the cluster or the audit sink
// (FUNC-SPEC O6): liveness must stay 200 even when the cluster is DOWN.
func Live(_ context.Context, _ *LiveInput) (*LiveOutput, error) {
	return &LiveOutput{Body: LiveBody{Status: "UP"}}, nil
}
