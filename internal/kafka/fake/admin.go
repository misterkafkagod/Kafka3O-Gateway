package fake

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// DescribeCluster returns the cluster id, controller, and seeded broker list
// (FUNC-SPEC §8.7 C1).
func (f *Fake) DescribeCluster(ctx context.Context) (kafka.ClusterInfo, error) {
	return invoke(f, ctx, "DescribeCluster", false, func() (kafka.ClusterInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		brokers := make([]kafka.Broker, len(f.model.brokers))
		copy(brokers, f.model.brokers)
		return kafka.ClusterInfo{
			ClusterID:    f.model.clusterID,
			ControllerID: f.model.controllerID,
			Brokers:      brokers,
		}, nil
	})
}

// compile-time proof that Fake satisfies the Admin surface it implements so far.
var _ kafka.Admin = (*Fake)(nil)
