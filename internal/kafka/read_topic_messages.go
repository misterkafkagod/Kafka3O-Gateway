package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type readTopicMessages struct {
	admin   *kadm.Client
	brokers []string
}

func NewReadTopicMessages(admin *kadm.Client, brokers []string) topics.FetchTopicMessagesFromPosition {
	return readTopicMessages{admin: admin, brokers: brokers}
}

func (r readTopicMessages) Execute(ctx context.Context, topic string, position topics.TopicMessagePosition) ([]topics.TopicMessage, error) {
	if position.Partition < 0 ||
		(position.Offset == nil) == (position.Timestamp == nil) ||
		(position.Offset != nil && *position.Offset < 0) ||
		(position.Timestamp != nil && *position.Timestamp < 0) {
		return nil, topics.ErrInvalidTopicMessagePosition
	}
	details, err := r.admin.ListTopicsWithInternal(ctx, topic)
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
	foundPartition := false
	for _, partition := range detail.Partitions.Sorted() {
		if partition.Partition == position.Partition {
			foundPartition = true
			break
		}
	}
	if !foundPartition {
		return nil, topics.ErrInvalidTopicMessagePosition
	}

	starts, err := r.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	start, ok := starts.Lookup(topic, position.Partition)
	if !ok {
		return nil, fmt.Errorf("missing start offset for partition %d", position.Partition)
	}
	ends, err := r.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	end, ok := ends.Lookup(topic, position.Partition)
	if !ok {
		return nil, fmt.Errorf("missing end offset for partition %d", position.Partition)
	}

	offset := int64(0)
	if position.Offset != nil {
		offset = *position.Offset
		if offset < start.Offset {
			return nil, topics.ErrInvalidTopicMessagePosition
		}
	} else {
		listed, err := r.admin.ListOffsetsAfterMilli(ctx, *position.Timestamp, topic)
		if err != nil {
			return nil, fmt.Errorf("find offset for timestamp: %w", err)
		}
		if err := listed.Error(); err != nil {
			return nil, fmt.Errorf("find offset for timestamp: %w", err)
		}
		resolved, ok := listed.Lookup(topic, position.Partition)
		if !ok {
			return nil, fmt.Errorf("missing timestamp offset for partition %d", position.Partition)
		}
		offset = resolved.Offset
	}
	if offset >= end.Offset {
		return []topics.TopicMessage{}, nil
	}
	return readTopicMessagesFromOffsets(
		ctx,
		r.brokers,
		topic,
		map[int32]int64{position.Partition: offset},
		map[int32]int64{position.Partition: end.Offset},
	)
}
