package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

type describePlacement struct {
	requester kmsg.Requestor
}

func NewDescribePlacement(requester kmsg.Requestor) partitions.DescribePlacement {
	return describePlacement{requester: requester}
}

func (d describePlacement) Execute(ctx context.Context, topic string) (partitions.Placement, error) {
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}
	response, err := request.RequestWith(ctx, d.requester)
	if err != nil {
		return partitions.Placement{}, fmt.Errorf("describe topic placement: %w", partitionError(err))
	}
	if response == nil {
		return partitions.Placement{}, errors.New("describe topic placement: empty Kafka response")
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return partitions.Placement{}, partitionError(err)
	}
	result := partitions.Placement{Brokers: make([]int32, 0, len(response.Brokers)), Partitions: make(map[int32][]int32)}
	for _, broker := range response.Brokers {
		result.Brokers = append(result.Brokers, broker.NodeID)
	}
	slices.Sort(result.Brokers)
	found := false
	for _, described := range response.Topics {
		if described.Topic == nil || *described.Topic != topic || found {
			return partitions.Placement{}, errors.New("describe topic placement: unexpected Kafka topic")
		}
		found = true
		if err := kerr.ErrorForCode(described.ErrorCode); err != nil {
			return partitions.Placement{}, partitionError(err)
		}
		for _, partition := range described.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return partitions.Placement{}, partitionError(err)
			}
			if _, duplicate := result.Partitions[partition.Partition]; duplicate {
				return partitions.Placement{}, errors.New("describe topic placement: duplicate partition")
			}
			result.Partitions[partition.Partition] = slices.Clone(partition.Replicas)
		}
	}
	if !found {
		return partitions.Placement{}, partitions.ErrNotFound
	}
	return result, nil
}
