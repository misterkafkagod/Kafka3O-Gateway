package topics

import "context"

// ListTopicMetadata is the Kafka port: it reads every topic's metadata, internal topics included.
// The franz-go adapter in internal/kafka implements it.
type ListTopicMetadata interface {
	Execute(ctx context.Context) ([]TopicMetadata, error)
}
