package partitions

import (
	"context"
	"slices"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type ListReassignments interface {
	Execute(ctx context.Context, topic string) ([]Reassignment, error)
}

type listReassignments struct {
	describe DescribePlacement
	list     ListKafkaReassignments
	timeout  time.Duration
}

func NewListReassignments(describe DescribePlacement, list ListKafkaReassignments, timeout time.Duration) ListReassignments {
	return listReassignments{describe: describe, list: list, timeout: timeout}
}

func (l listReassignments) Execute(ctx context.Context, topic string) ([]Reassignment, error) {
	if !topics.IsValidTopicName(topic) {
		return nil, ErrInvalidChange
	}
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	placement, err := l.describe.Execute(ctx, topic)
	if err != nil {
		return nil, err
	}
	ids := make([]int32, 0, len(placement.Partitions))
	for id := range placement.Partitions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return l.list.Execute(ctx, topic, ids)
}
