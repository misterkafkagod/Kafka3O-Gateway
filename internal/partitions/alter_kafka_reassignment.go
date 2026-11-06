package partitions

import "context"

type AlterKafkaReassignment interface {
	Execute(ctx context.Context, topic string, partition int32, replicas []int32) error
}
