package partitions

import "context"

type ListKafkaReassignments interface {
	Execute(ctx context.Context, topic string, partitionIDs []int32) ([]Reassignment, error)
}
