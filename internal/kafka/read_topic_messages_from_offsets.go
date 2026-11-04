package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func readTopicMessagesFromOffsets(
	ctx context.Context,
	brokers []string,
	topic string,
	offsets map[int32]int64,
	endOffsets map[int32]int64,
) ([]topics.TopicMessage, error) {
	consumerOffsets := make(map[int32]kgo.Offset, len(offsets))
	for partition, offset := range offsets {
		consumerOffsets[partition] = kgo.NewOffset().At(offset)
	}

	reader, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: consumerOffsets}),
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
		var partitionErr error
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if partition.Topic != topic {
				return
			}
			end, ok := endOffsets[partition.Partition]
			if !ok {
				return
			}
			if partition.Err != nil {
				partitionErr = fmt.Errorf("fetch partition %d: %w", partition.Partition, partition.Err)
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
		if partitionErr != nil {
			return nil, partitionErr
		}
	}
	return messages, nil
}
