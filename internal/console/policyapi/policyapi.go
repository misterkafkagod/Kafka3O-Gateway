// Package policyapi serves the Console's mutual-TLS policy endpoint that secured Gateways call before Kafka.
package policyapi

import (
	"context"
	"errors"
)

// ConsumePath is the only route of the policy listener.
const ConsumePath = "/internal/v1/delegations/consume"

type RegisteredClusters interface {
	ActiveRegisteredCluster(ctx context.Context, clusterID string) (bool, error)
}

var ErrConfig = errors.New("policy API dependencies are required")
