package kafka

import (
	"context"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type brokerLeadersAdmin interface {
	Metadata(context.Context, ...string) (kadm.Metadata, error)
}

type readBrokerLeaders struct {
	admin brokerLeadersAdmin
}

// NewReadBrokerLeaders counts leaders and replicas per broker from one metadata read (the quorum-and-leader-metrics
// recipe); the preferred replica is a partition's first replica.
func NewReadBrokerLeaders(admin brokerLeadersAdmin) metrics.ReadBrokerLeaders {
	return readBrokerLeaders{admin: admin}
}

func (r readBrokerLeaders) Execute(ctx context.Context) ([]metrics.BrokerLeaders, error) {
	metadata, err := r.admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}
	counts := map[int32]*metrics.BrokerLeaders{}
	broker := func(id int32) *metrics.BrokerLeaders {
		if counts[id] == nil {
			counts[id] = &metrics.BrokerLeaders{Broker: id}
		}
		return counts[id]
	}
	for _, detail := range metadata.Brokers {
		broker(detail.NodeID)
	}
	metadata.Topics.EachPartition(func(partition kadm.PartitionDetail) {
		for _, replica := range partition.Replicas {
			broker(replica).Replicas++
		}
		if partition.Leader < 0 || partition.Err != nil {
			return
		}
		broker(partition.Leader).Leaders++
		if len(partition.Replicas) > 0 && partition.Replicas[0] != partition.Leader {
			broker(partition.Leader).NonPreferredLeaders++
		}
	})
	result := make([]metrics.BrokerLeaders, 0, len(counts))
	for _, count := range counts {
		result = append(result, *count)
	}
	slices.SortFunc(result, metrics.CompareBrokerLeaders)
	return result, nil
}
