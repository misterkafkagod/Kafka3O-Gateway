// Package api serves the gateway's HTTP API. Handlers depend only on the
// Cluster interface; the Kafka adapter in internal/kafka implements it.
package api

import "context"

// Cluster is everything the HTTP layer needs from Kafka.
type Cluster interface {
	// Ping checks that the cluster is reachable.
	Ping(ctx context.Context) error
	// DescribeCluster returns the cluster id, controller and brokers.
	DescribeCluster(ctx context.Context) (ClusterInfo, error)
}

// ClusterInfo describes the cluster as a whole.
type ClusterInfo struct {
	ClusterID    string   `json:"clusterId"`
	ControllerID int32    `json:"controllerId"`
	Brokers      []Broker `json:"brokers"`
}

// Broker is one broker of the cluster.
type Broker struct {
	ID   int32   `json:"id"`
	Host string  `json:"host"`
	Port int32   `json:"port"`
	Rack *string `json:"rack"`
}
