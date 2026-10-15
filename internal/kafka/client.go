// Package kafka adapts franz-go to the api.Cluster interface. It is the only
// package that imports franz-go.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/api"
)

// Client implements api.Cluster for one Kafka cluster.
type Client struct {
	kgo *kgo.Client
	adm *kadm.Client
}

var _ api.Cluster = (*Client)(nil)

// New creates a client for the given seed brokers. It does not connect until
// the first request.
func New(brokers []string) (*Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Client{kgo: cl, adm: kadm.NewClient(cl)}, nil
}

// Close releases the client's connections.
func (c *Client) Close() { c.kgo.Close() }

// Ping checks that at least one broker answers before ctx expires.
func (c *Client) Ping(ctx context.Context) error {
	return c.kgo.Ping(ctx)
}

// DescribeCluster returns the cluster id, controller and brokers.
func (c *Client) DescribeCluster(ctx context.Context) (api.ClusterInfo, error) {
	m, err := c.adm.BrokerMetadata(ctx)
	if err != nil {
		return api.ClusterInfo{}, err
	}
	info := api.ClusterInfo{
		ClusterID:    m.Cluster,
		ControllerID: m.Controller,
		Brokers:      make([]api.Broker, 0, len(m.Brokers)),
	}
	for _, b := range m.Brokers {
		info.Brokers = append(info.Brokers, api.Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Rack: b.Rack})
	}
	return info, nil
}
