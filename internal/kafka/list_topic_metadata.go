package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type listTopicMetadata struct {
	admin *kadm.Client
}

// NewListTopicMetadata adapts kadm topic metadata to the topics.ListTopicMetadata port.
func NewListTopicMetadata(admin *kadm.Client) topics.ListTopicMetadata {
	return listTopicMetadata{admin: admin}
}

func (l listTopicMetadata) Execute(ctx context.Context) ([]topics.TopicMetadata, error) {
	details, err := l.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	if err := details.Error(); err != nil {
		return nil, fmt.Errorf("load topic: %w", err)
	}
	result := make([]topics.TopicMetadata, 0, len(details))
	for _, d := range details {
		var replicationFactor int32
		for _, p := range d.Partitions {
			replicationFactor = max(replicationFactor, int32(len(p.Replicas)))
		}
		result = append(result, topics.TopicMetadata{
			Name:              d.Topic,
			Partitions:        int32(len(d.Partitions)),
			ReplicationFactor: replicationFactor,
			Internal:          d.IsInternal,
		})
	}
	return result, nil
}
