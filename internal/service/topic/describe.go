package topic

import (
	"context"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// PartitionDetail is one partition in a Describe result (FUNC-SPEC §8.7 T2).
type PartitionDetail struct {
	ID          int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
	BeginOffset int64
	EndOffset   int64
	// ApproxCount is EndOffset - BeginOffset: an upper bound on the
	// partition's message count on a compacted topic (FUNC-SPEC §8.7 T2).
	ApproxCount int64
}

// Describe is Describe's result (FUNC-SPEC §8.7 T2).
type Describe struct {
	Name               string
	Internal           bool
	ReplicationFactor  int
	ApproxMessageCount int64
	Partitions         []PartitionDetail
	Configs            []kafka.ConfigEntry
}

// Describe returns one topic's partitions (leader, replicas, ISR, begin/end
// offsets, approximate count) and configuration properties (FUNC-SPEC §8.7
// T2). ApproxMessageCount is the sum of every partition's ApproxCount.
// Unknown topic → the port's NotFound error, unchanged.
func (s *Service) Describe(ctx context.Context, name string) (Describe, error) {
	t, err := s.admin.DescribeTopics(ctx, name)
	if err != nil {
		return Describe{}, err
	}

	begin, err := s.admin.ListStartOffsets(ctx, name)
	if err != nil {
		return Describe{}, err
	}
	end, err := s.admin.ListEndOffsets(ctx, name)
	if err != nil {
		return Describe{}, err
	}
	configs, err := s.admin.DescribeTopicConfigs(ctx, name)
	if err != nil {
		return Describe{}, err
	}

	partitions := make([]PartitionDetail, len(t.Partitions))
	var approxTotal int64
	for i, p := range t.Partitions {
		b := begin[p.ID]
		e := end[p.ID]
		approx := e - b
		approxTotal += approx
		partitions[i] = PartitionDetail{
			ID:          p.ID,
			Leader:      p.Leader,
			Replicas:    p.Replicas,
			ISR:         p.ISR,
			BeginOffset: b,
			EndOffset:   e,
			ApproxCount: approx,
		}
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })

	return Describe{
		Name:               t.Name,
		Internal:           t.Internal,
		ReplicationFactor:  t.ReplicationFactor,
		ApproxMessageCount: approxTotal,
		Partitions:         partitions,
		Configs:            configs,
	}, nil
}
