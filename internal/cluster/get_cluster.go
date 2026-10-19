package cluster

import (
	"cmp"
	"context"
	"slices"
	"time"
)

// GetCluster returns the cluster identity with brokers sorted by ID.
type GetCluster interface {
	Execute(ctx context.Context) (Cluster, error)
}

type getCluster struct {
	describe DescribeCluster
	timeout  time.Duration
}

func NewGetCluster(describe DescribeCluster, timeout time.Duration) GetCluster {
	return getCluster{describe: describe, timeout: timeout}
}

func (g getCluster) Execute(ctx context.Context) (Cluster, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	c, err := g.describe.Execute(ctx)
	if err != nil {
		return Cluster{}, err
	}
	c.Brokers = slices.Clone(c.Brokers)
	slices.SortFunc(c.Brokers, func(a, b Broker) int { return cmp.Compare(a.ID, b.ID) })
	return c, nil
}
