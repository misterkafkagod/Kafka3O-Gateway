package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
)

type describeCluster struct {
	admin *kadm.Client
}

// NewDescribeCluster adapts kadm broker metadata to the cluster.DescribeCluster port.
func NewDescribeCluster(admin *kadm.Client) cluster.DescribeCluster {
	return describeCluster{admin: admin}
}

func (d describeCluster) Execute(ctx context.Context) (cluster.Cluster, error) {
	metadata, err := d.admin.BrokerMetadata(ctx)
	if err != nil {
		return cluster.Cluster{}, fmt.Errorf("read broker metadata: %w", err)
	}
	brokers := make([]cluster.Broker, 0, len(metadata.Brokers))
	for _, b := range metadata.Brokers {
		brokers = append(brokers, cluster.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack})
	}
	return cluster.Cluster{ID: metadata.Cluster, ControllerID: metadata.Controller, Brokers: brokers}, nil
}
