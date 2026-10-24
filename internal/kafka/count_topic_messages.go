package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type countTopicMessages struct {
	admin *kadm.Client
}

// NewCountTopicMessages adapts kadm start/end offsets to the topics.CountTopicMessages port.
// The count is approximate: compaction, deleted records and transaction markers skew it.
func NewCountTopicMessages(admin *kadm.Client) topics.CountTopicMessages {
	return countTopicMessages{admin: admin}
}

func (c countTopicMessages) Execute(ctx context.Context, names []string) (map[string]int64, error) {
	counts := make(map[string]int64, len(names))
	// kadm lists every topic when none are named; nothing to count is nothing to ask.
	if len(names) == 0 {
		return counts, nil
	}
	starts, err := c.admin.ListStartOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	ends.Each(func(end kadm.ListedOffset) {
		start, _ := starts.Lookup(end.Topic, end.Partition)
		counts[end.Topic] += max(end.Offset-start.Offset, 0)
	})
	return counts, nil
}
