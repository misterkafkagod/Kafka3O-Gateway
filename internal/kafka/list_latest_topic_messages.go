package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

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
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := l.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}

	offsets := make(map[int32]kgo.Offset, len(detail.Partitions))
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
		offset := max(start.Offset, end.Offset-50)
		offsets[partition.Partition] = kgo.NewOffset().At(offset)
		endOffsets[partition.Partition] = end.Offset
	}
	if len(offsets) == 0 {
		return []topics.TopicMessage{}, nil
	}

	reader, err := kgo.NewClient(
		kgo.SeedBrokers(l.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create topic reader: %w", err)
	}
	defer reader.Close()

	done := make(map[int32]bool, len(endOffsets))
	messages := make([]topics.TopicMessage, 0, len(offsets)*50)
	for len(done) < len(endOffsets) {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return nil, fmt.Errorf("fetch topic messages: %w", err)
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if partition.Topic != topic {
				return
			}
			end, ok := endOffsets[partition.Partition]
			if !ok {
				return
			}
			if partition.Err != nil {
				err = fmt.Errorf("fetch partition %d: %w", partition.Partition, partition.Err)
				return
			}
			for _, record := range partition.Records {
				if record.Offset >= end {
					done[partition.Partition] = true
					continue
				}
				messages = append(messages, topicMessageFromRecord(record))
				if record.Offset >= end-1 {
					done[partition.Partition] = true
				}
			}
			if len(partition.Records) == 0 && partition.HighWatermark >= end {
				done[partition.Partition] = true
			}
		})
		if err != nil {
			return nil, err
		}
	}
	return messages, nil
}
