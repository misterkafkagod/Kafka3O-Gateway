package partitions

import (
	"context"
	"fmt"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type IncreasePartitionCount interface {
	Execute(ctx context.Context, topic string, count int32) error
}

type increasePartitionCount struct {
	describe DescribePlacement
	update   UpdateKafkaPartitions
	timeout  time.Duration
}

func NewIncreasePartitionCount(describe DescribePlacement, update UpdateKafkaPartitions, timeout time.Duration) IncreasePartitionCount {
	return increasePartitionCount{describe: describe, update: update, timeout: timeout}
}

func (i increasePartitionCount) Execute(ctx context.Context, topic string, count int32) error {
	if !topics.IsValidTopicName(topic) || count < 1 {
		return ErrInvalidChange
	}
	ctx, cancel := context.WithTimeout(ctx, i.timeout)
	defer cancel()
	placement, err := i.describe.Execute(ctx, topic)
	if err != nil {
		return err
	}
	if int64(count) <= int64(len(placement.Partitions)) {
		return ErrInvalidChange
	}
	if err := i.update.Execute(ctx, topic, count, true); err != nil {
		return err
	}
	if err := i.update.Execute(ctx, topic, count, false); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteUnknown, err)
	}
	return nil
}
