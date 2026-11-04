package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/cluster"
)

type describeCluster struct {
	requester kmsg.Requestor
}

// NewDescribeCluster adapts live broker metadata to the cluster.DescribeCluster port.
func NewDescribeCluster(requester kmsg.Requestor) cluster.DescribeCluster {
	return describeCluster{requester: requester}
}

func (d describeCluster) Execute(ctx context.Context) (cluster.Cluster, error) {
	metadata, err := requestMetadata(ctx, d.requester, false)
	if err != nil {
		return cluster.Cluster{}, fmt.Errorf("read broker metadata: %w", err)
	}
	brokers := make([]cluster.Broker, 0, len(metadata.Brokers))
	for _, b := range metadata.Brokers {
		brokers = append(brokers, cluster.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack})
	}
	var clusterID string
	if metadata.ClusterID != nil {
		clusterID = *metadata.ClusterID
	}
	return cluster.Cluster{ID: clusterID, ControllerID: metadata.ControllerID, Brokers: brokers}, nil
}
