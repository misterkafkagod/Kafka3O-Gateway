package kafka

import (
	"context"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

type listReassignmentsAdmin interface {
	ListPartitionReassignments(context.Context, kadm.TopicsSet) (kadm.ListPartitionReassignmentsResponses, error)
}

type listKafkaReassignments struct {
	admin listReassignmentsAdmin
}

func NewListKafkaReassignments(admin listReassignmentsAdmin) partitions.ListKafkaReassignments {
	return listKafkaReassignments{admin: admin}
}

func (l listKafkaReassignments) Execute(ctx context.Context, topic string, partitionIDs []int32) ([]partitions.Reassignment, error) {
	result := make([]partitions.Reassignment, 0)
	if len(partitionIDs) == 0 {
		return result, nil
	}
	var requested kadm.TopicsSet
	requested.Add(topic, partitionIDs...)
	responses, err := l.admin.ListPartitionReassignments(ctx, requested)
	if err != nil {
		return nil, fmt.Errorf("list reassignments: %w", partitionError(err))
	}
	for _, response := range responses.Sorted() {
		if response.Topic != topic || !slices.Contains(partitionIDs, response.Partition) {
			return nil, fmt.Errorf("list reassignments: unexpected Kafka partition")
		}
		result = append(result, partitions.Reassignment{
			Partition:        response.Partition,
			Replicas:         append([]int32{}, response.Replicas...),
			AddingReplicas:   append([]int32{}, response.AddingReplicas...),
			RemovingReplicas: append([]int32{}, response.RemovingReplicas...),
		})
	}
	return result, nil
}
