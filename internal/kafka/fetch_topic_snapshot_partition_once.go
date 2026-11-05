package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (f *topicSnapshotFetcher) fetchOnce(ctx context.Context, partition int32, offset int64) (fetchedTopicPartition, error) {
	metadata, ok := f.detail.Partitions[partition]
	if !ok {
		return fetchedTopicPartition{}, fmt.Errorf("missing metadata for partition %d", partition)
	}
	if metadata.Err != nil {
		return fetchedTopicPartition{}, metadata.Err
	}
	if metadata.Leader < 0 {
		return fetchedTopicPartition{}, kerr.LeaderNotAvailable
	}
	request := kmsg.NewPtrFetchRequest()
	request.MinBytes = 1
	request.MaxBytes = 50 << 20
	requestTopic := kmsg.NewFetchRequestTopic()
	requestTopic.Topic = f.topic
	requestTopic.TopicID = [16]byte(f.detail.ID)
	requestPartition := kmsg.NewFetchRequestTopicPartition()
	requestPartition.Partition = partition
	requestPartition.CurrentLeaderEpoch = metadata.LeaderEpoch
	requestPartition.FetchOffset = offset
	requestPartition.PartitionMaxBytes = 1 << 20
	requestTopic.Partitions = []kmsg.FetchRequestTopicPartition{requestPartition}
	request.Topics = []kmsg.FetchRequestTopic{requestTopic}

	response, err := request.RequestWith(ctx, f.reader.Broker(int(metadata.Leader)))
	if err != nil {
		return fetchedTopicPartition{}, err
	}
	return f.parseResponse(response, partition, offset)
}
