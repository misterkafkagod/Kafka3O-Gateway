package metrics

import "context"

// ReadPartitionHealth reads every topic's partition health, internal topics included.
type ReadPartitionHealth interface {
	Execute(ctx context.Context) ([]TopicHealth, error)
}
