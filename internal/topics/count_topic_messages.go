package topics

import "context"

// CountTopicMessages is the Kafka port: it returns the approximate message count per topic name.
// The franz-go adapter in internal/kafka implements it.
type CountTopicMessages interface {
	Execute(ctx context.Context, names []string) (map[string]int64, error)
}
