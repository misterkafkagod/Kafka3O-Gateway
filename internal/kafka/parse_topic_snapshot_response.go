package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (f *topicSnapshotFetcher) parseResponse(response *kmsg.FetchResponse, partition int32, offset int64) (fetchedTopicPartition, error) {
	if response == nil {
		return fetchedTopicPartition{}, fmt.Errorf("fetch returned no response")
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return fetchedTopicPartition{}, err
	}
	if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 {
		return fetchedTopicPartition{}, fmt.Errorf("fetch returned an unexpected topic or partition count")
	}
	responseTopic := response.Topics[0]
	if (response.Version < 13 && responseTopic.Topic != f.topic) ||
		(response.Version >= 13 && responseTopic.TopicID != [16]byte(f.detail.ID)) {
		return fetchedTopicPartition{}, fmt.Errorf("fetch returned an unexpected topic")
	}
	responsePartition := &responseTopic.Partitions[0]
	if responsePartition.Partition != partition {
		return fetchedTopicPartition{}, fmt.Errorf("fetch returned unexpected partition %d", responsePartition.Partition)
	}
	if err := kerr.ErrorForCode(responsePartition.ErrorCode); err != nil {
		return fetchedTopicPartition{}, err
	}
	parsed, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
		KeepControlRecords: true,
		Offset:             offset,
		IsolationLevel:     kgo.ReadUncommitted(),
		Topic:              f.topic,
		Partition:          partition,
	}, responsePartition, f.decompressor, nil)
	if parsed.Err != nil {
		return fetchedTopicPartition{}, parsed.Err
	}
	return fetchedTopicPartition{partition: parsed, next: next, hasBatch: len(responsePartition.RecordBatches) != 0}, nil
}
