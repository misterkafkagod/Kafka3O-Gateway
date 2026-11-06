package partitions

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type ReassignPartition interface {
	Execute(ctx context.Context, topic string, partition int32, replicas []int32) error
}

type reassignPartition struct {
	describe DescribePlacement
	list     ListKafkaReassignments
	alter    AlterKafkaReassignment
	timeout  time.Duration
}

func NewReassignPartition(describe DescribePlacement, list ListKafkaReassignments, alter AlterKafkaReassignment, timeout time.Duration) ReassignPartition {
	return reassignPartition{describe: describe, list: list, alter: alter, timeout: timeout}
}

func (r reassignPartition) Execute(ctx context.Context, topic string, partition int32, replicas []int32) error {
	if !topics.IsValidTopicName(topic) || partition < 0 || len(replicas) == 0 {
		return ErrInvalidChange
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	placement, err := r.describe.Execute(ctx, topic)
	if err != nil {
		return err
	}
	current, ok := placement.Partitions[partition]
	if !ok {
		return ErrNotFound
	}
	if slices.Equal(current, replicas) {
		return ErrInvalidChange
	}
	seen := make(map[int32]struct{}, len(replicas))
	for _, broker := range replicas {
		if broker < 0 || !slices.Contains(placement.Brokers, broker) {
			return ErrInvalidChange
		}
		if _, exists := seen[broker]; exists {
			return ErrInvalidChange
		}
		seen[broker] = struct{}{}
	}
	active, err := r.list.Execute(ctx, topic, []int32{partition})
	if err != nil {
		return err
	}
	if len(active) != 0 {
		return ErrInProgress
	}
	if err := r.alter.Execute(ctx, topic, partition, replicas); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteUnknown, err)
	}
	return nil
}
