package groups

import "context"

type ConsumerPartitionSnapshot struct {
	Topic           string
	Partition       int32
	CommittedOffset *int64
	StartOffset     *int64
	EndOffset       *int64
	Error           string
}

type ConsumerGroupSnapshot struct {
	ID         string
	Protocol   string
	State      string
	Members    []ConsumerGroupMember
	Topics     []string
	Partitions []ConsumerPartitionSnapshot
	Error      string
}

// FetchConsumerGroups retrieves group membership and the offsets needed for lag.
type FetchConsumerGroups interface {
	Execute(ctx context.Context) ([]ConsumerGroupSnapshot, error)
}
