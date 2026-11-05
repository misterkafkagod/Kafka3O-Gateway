package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fetchRecentTopicMessages struct {
	admin  *kadm.Client
	client *kgo.Client
}

func NewFetchRecentTopicMessages(admin *kadm.Client, client *kgo.Client) topics.FetchRecentTopicMessages {
	return fetchRecentTopicMessages{admin: admin, client: client}
}

func (f fetchRecentTopicMessages) Execute(ctx context.Context, topic string) (topics.TopicMessageWindow, error) {
	details, err := f.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("load topic metadata: %w", err)
	}
	detail, ok := details[topic]
	if !ok || detail.Err == kerr.UnknownTopicOrPartition {
		return topics.TopicMessageWindow{}, topics.ErrTopicNotFound
	}
	if detail.Err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("load topic metadata: %w", detail.Err)
	}

	startOffsets, err := f.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("list start offsets: %w", topicOffsetError(ctx, f.admin, topic, err))
	}
	if err := startOffsets.Error(); err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("list start offsets: %w", topicOffsetError(ctx, f.admin, topic, err))
	}
	endOffsets, err := f.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("list end offsets: %w", topicOffsetError(ctx, f.admin, topic, err))
	}
	if err := endOffsets.Error(); err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("list end offsets: %w", topicOffsetError(ctx, f.admin, topic, err))
	}
	starts := make(map[int32]int64, len(detail.Partitions))
	ends := make(map[int32]int64, len(detail.Partitions))
	for _, partition := range detail.Partitions.Sorted() {
		start, ok := startOffsets.Lookup(topic, partition.Partition)
		if !ok {
			return topics.TopicMessageWindow{}, fmt.Errorf("missing start offset for partition %d", partition.Partition)
		}
		end, ok := endOffsets.Lookup(topic, partition.Partition)
		if !ok {
			return topics.TopicMessageWindow{}, fmt.Errorf("missing end offset for partition %d", partition.Partition)
		}
		starts[partition.Partition] = start.Offset
		ends[partition.Partition] = end.Offset
	}
	selectedStarts, selectedEnds, truncated, err := allocateRecentTopicOffsets(starts, ends, topics.TopicMessageSearchLimit)
	if err != nil {
		return topics.TopicMessageWindow{}, fmt.Errorf("allocate search window: %w", err)
	}
	if len(selectedStarts) == 0 {
		return topics.TopicMessageWindow{Messages: []topics.TopicMessage{}, Truncated: truncated}, nil
	}
	messages, err := readTopicMessagesFromOffsets(ctx, f.client.Opts(), topic, detail, selectedStarts, selectedEnds, topics.TopicMessageSearchLimit)
	if err != nil {
		return topics.TopicMessageWindow{}, err
	}
	return topics.TopicMessageWindow{Messages: messages, Truncated: truncated}, nil
}
