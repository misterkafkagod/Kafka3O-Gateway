package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type listTopicMetadata struct {
	requester kmsg.Requestor
}

// NewListTopicMetadata adapts live topic metadata to the topics.ListTopicMetadata port.
func NewListTopicMetadata(requester kmsg.Requestor) topics.ListTopicMetadata {
	return listTopicMetadata{requester: requester}
}

func (l listTopicMetadata) Execute(ctx context.Context) ([]topics.TopicMetadata, error) {
	metadata, err := requestMetadata(ctx, l.requester, true)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	result := make([]topics.TopicMetadata, 0, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		var name string
		if topic.Topic != nil {
			name = *topic.Topic
		}
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
			return nil, fmt.Errorf("load topic %q: %w", name, err)
		}
		if topic.Topic == nil {
			return nil, fmt.Errorf("load topic: metadata response has no topic name")
		}
		var replicationFactor int32
		for _, p := range topic.Partitions {
			replicationFactor = max(replicationFactor, int32(len(p.Replicas)))
		}
		result = append(result, topics.TopicMetadata{
			Name:              name,
			Partitions:        int32(len(topic.Partitions)),
			ReplicationFactor: replicationFactor,
			Internal:          topic.IsInternal,
		})
	}
	return result, nil
}
