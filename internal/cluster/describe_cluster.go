package cluster

import "context"

// DescribeCluster is the Kafka port: it reads the cluster ID, controller and brokers.
// The franz-go adapter in internal/kafka implements it.
type DescribeCluster interface {
	Execute(ctx context.Context) (Cluster, error)
}
