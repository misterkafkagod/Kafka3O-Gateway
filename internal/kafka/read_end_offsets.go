package kafka

import (
	"context"
	"fmt"
	"math/big"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type endOffsetsAdmin interface {
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
}

type readEndOffsets struct {
	admin endOffsetsAdmin
}

// NewReadEndOffsets adapts ListOffsets (latest) to the metrics.ReadEndOffsets port.
func NewReadEndOffsets(admin endOffsetsAdmin) metrics.ReadEndOffsets {
	return readEndOffsets{admin: admin}
}

func (r readEndOffsets) Execute(ctx context.Context, topics []string) ([]metrics.TopicEndOffset, error) {
	if len(topics) == 0 {
		return []metrics.TopicEndOffset{}, nil
	}
	listed, err := r.admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	sums := map[string]*big.Int{}
	failures := map[string]string{}
	listed.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil {
			failures[offset.Topic] = fmt.Sprintf("partition %d: %v", offset.Partition, offset.Err)
			return
		}
		if sums[offset.Topic] == nil {
			sums[offset.Topic] = new(big.Int)
		}
		sums[offset.Topic].Add(sums[offset.Topic], big.NewInt(offset.Offset))
	})
	result := make([]metrics.TopicEndOffset, 0, len(topics))
	for _, topic := range topics {
		switch {
		case failures[topic] != "":
			result = append(result, metrics.TopicEndOffset{Topic: topic, Error: failures[topic]})
		case sums[topic] != nil:
			result = append(result, metrics.TopicEndOffset{Topic: topic, EndOffset: sums[topic].String()})
		default:
			result = append(result, metrics.TopicEndOffset{Topic: topic, Error: "no end offsets were reported"})
		}
	}
	slices.SortFunc(result, metrics.CompareTopicEndOffsets)
	return result, nil
}
