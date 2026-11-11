package metrics

import "context"

// ReadBrokerLeaders reads, per broker, the partitions it leads, the replicas it holds and its non-preferred leaders.
type ReadBrokerLeaders interface {
	Execute(ctx context.Context) ([]BrokerLeaders, error)
}
