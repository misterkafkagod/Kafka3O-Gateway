package partitions

import "context"

type UpdateKafkaPartitions interface {
	Execute(ctx context.Context, topic string, count int32, validateOnly bool) error
}
