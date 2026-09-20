package franz

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// DescribeCluster returns the cluster id, controller, and broker list
// (FUNC-SPEC §8.7 C1) via kadm.BrokerMetadata.
func (c *Client) DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error) {
	md, err := c.kadm.BrokerMetadata(ctx)
	if err != nil {
		return kafka.ClusterInfo{}, wrapErr("", err)
	}
	brokers := make([]kafka.Broker, len(md.Brokers))
	for i, b := range md.Brokers {
		var rack string
		if b.Rack != nil {
			rack = *b.Rack
		}
		brokers[i] = kafka.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: rack}
	}
	return kafka.ClusterInfo{ClusterID: md.Cluster, ControllerID: md.Controller, Brokers: brokers}, nil
}

// compile-time proof that Client satisfies the Admin surface it implements so far.
var _ kafka.Admin = (*Client)(nil)
