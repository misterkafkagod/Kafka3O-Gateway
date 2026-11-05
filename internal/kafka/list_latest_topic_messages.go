package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type listLatestTopicMessages struct {
	admin   *kadm.Client
	brokers []string
}

func NewListLatestTopicMessages(admin *kadm.Client, brokers []string) topics.FetchTopicMessages {
	return listLatestTopicMessages{admin: admin, brokers: brokers}
}

func (l listLatestTopicMessages) Execute(ctx context.Context, topic string) ([]topics.TopicMessage, error) {
	details, err := l.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("load topic metadata: %w", err)
	}
	detail, ok := details[topic]
	if !ok || detail.Err == kerr.UnknownTopicOrPartition {
		return nil, topics.ErrTopicNotFound
	}
	if detail.Err != nil {
		return nil, fmt.Errorf("load topic metadata: %w", detail.Err)
	}

	starts, err := l.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", topicOffsetError(ctx, l.admin, topic, err))
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("list start offsets: %w", topicOffsetError(ctx, l.admin, topic, err))
	}
	ends, err := l.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", topicOffsetError(ctx, l.admin, topic, err))
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("list end offsets: %w", topicOffsetError(ctx, l.admin, topic, err))
	}

	offsets := make(map[int32]int64, len(detail.Partitions))
	endOffsets := make(map[int32]int64, len(detail.Partitions))
	for _, partition := range detail.Partitions.Sorted() {
		start, ok := starts.Lookup(topic, partition.Partition)
		if !ok {
			return nil, fmt.Errorf("missing start offset for partition %d", partition.Partition)
		}
		end, ok := ends.Lookup(topic, partition.Partition)
		if !ok {
			return nil, fmt.Errorf("missing end offset for partition %d", partition.Partition)
		}
		if start.Offset > end.Offset {
			return nil, fmt.Errorf("start offset exceeds end offset for partition %d", partition.Partition)
		}
		offset := max(start.Offset, end.Offset-50)
		if offset == end.Offset {
			continue
		}
		offsets[partition.Partition] = offset
		endOffsets[partition.Partition] = end.Offset
	}
	if len(offsets) == 0 {
		return []topics.TopicMessage{}, nil
	}
	return readTopicMessagesFromOffsets(ctx, l.brokers, topic, detail, offsets, endOffsets, topicMessagesFetchLimit)
}
