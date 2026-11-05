package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

const topicMessagesFetchLimit = 50

const topicMessagesProgressWait = 500 * time.Millisecond

func readTopicMessagesFromOffsets(
	ctx context.Context,
	clientOptions []kgo.Opt,
	topic string,
	detail kadm.TopicDetail,
	offsets map[int32]int64,
	endOffsets map[int32]int64,
	maxMessagesPerPartition int,
) ([]topics.TopicMessage, error) {
	state, err := newTopicMessageReadState(topic, offsets, endOffsets, maxMessagesPerPartition)
	if err != nil {
		return nil, err
	}
	consumerOffsets := make(map[int32]kgo.Offset, len(offsets))
	for partition, offset := range offsets {
		if !state.done[partition] {
			consumerOffsets[partition] = kgo.NewOffset().At(offset)
		}
	}
	if len(consumerOffsets) == 0 {
		return state.messages, nil
	}

	opts := append([]kgo.Opt(nil), clientOptions...)
	opts = append(opts,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: consumerOffsets}),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.KeepControlRecords(),
	)
	if detail.ID == (kadm.TopicID{}) {
		opts = append(opts, kgo.MaxVersions(kversion.V2_8_0()))
	}
	reader, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create topic reader: %w", err)
	}
	defer reader.Close()

	for len(state.done) < len(endOffsets) {
		pollCtx, cancel := context.WithTimeout(ctx, topicMessagesProgressWait)
		fetches := reader.PollFetches(pollCtx)
		cancel()
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				// PollFetches drops empty fetches; inspect remaining partitions directly.
				if err := finishTopicMessageRead(ctx, reader, detail, state); err != nil {
					return nil, err
				}
				return state.messages, nil
			}
			return nil, fmt.Errorf("fetch topic messages: %w", topicOffsetError(ctx, kadm.NewClient(reader), topic, err))
		}
		var partitionErr error
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if partitionErr == nil {
				partitionErr = state.accept(partition)
			}
		})
		if partitionErr != nil {
			return nil, partitionErr
		}
	}
	return state.messages, nil
}
