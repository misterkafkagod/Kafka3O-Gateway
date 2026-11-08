// Package policyapi serves the Console's mutual-TLS policy endpoint that secured Gateways call before Kafka.
package policyapi

import (
	"context"
	"errors"
)

// ConsumePath and OutcomePath are the only routes of the policy listener.
const (
	ConsumePath = "/internal/v1/delegations/consume"
	OutcomePath = "/internal/v1/delegations/outcome"
)

type RegisteredClusters interface {
	ActiveRegisteredCluster(ctx context.Context, clusterID string) (bool, error)
}

var ErrConfig = errors.New("policy API dependencies are required")
