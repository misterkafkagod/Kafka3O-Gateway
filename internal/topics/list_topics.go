package topics

import (
	"context"
	"slices"
	"time"
)

// ListTopics returns every topic with its approximate message count, sorted by name.
type ListTopics interface {
	Execute(ctx context.Context) ([]Topic, error)
}

type listTopics struct {
	metadata ListTopicMetadata
	count    CountTopicMessages
	timeout  time.Duration
}

func NewListTopics(metadata ListTopicMetadata, count CountTopicMessages, timeout time.Duration) ListTopics {
	return listTopics{metadata: metadata, count: count, timeout: timeout}
}

func (l listTopics) Execute(ctx context.Context) ([]Topic, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	metadata, err := l.metadata.Execute(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(metadata))
	for _, m := range metadata {
		names = append(names, m.Name)
	}
	counts, err := l.count.Execute(ctx, names)
	if err != nil {
		return nil, err
	}
	result := make([]Topic, 0, len(metadata))
	for _, m := range metadata {
		result = append(result, Topic{
			Name:              m.Name,
			Partitions:        m.Partitions,
			ReplicationFactor: m.ReplicationFactor,
			Internal:          m.Internal,
			ApproxMessages:    counts[m.Name],
		})
	}
	slices.SortFunc(result, CompareTopicsByName)
	return result, nil
}
