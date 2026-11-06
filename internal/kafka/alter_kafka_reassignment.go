package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

type alterReassignmentAdmin interface {
	AlterPartitionAssignments(context.Context, kadm.AlterPartitionAssignmentsReq) (kadm.AlterPartitionAssignmentsResponses, error)
}

type alterKafkaReassignment struct {
	admin alterReassignmentAdmin
}

func NewAlterKafkaReassignment(admin alterReassignmentAdmin) partitions.AlterKafkaReassignment {
	return alterKafkaReassignment{admin: admin}
}

func (a alterKafkaReassignment) Execute(ctx context.Context, topic string, partition int32, replicas []int32) error {
	if len(replicas) == 0 {
		return partitions.ErrInvalidChange
	}
	var request kadm.AlterPartitionAssignmentsReq
	request.Assign(topic, partition, replicas)
	responses, err := a.admin.AlterPartitionAssignments(ctx, request)
	if err != nil {
		return fmt.Errorf("alter reassignment: %w", partitionError(err))
	}
	response, ok := responses[topic][partition]
	if !ok {
		return errors.New("alter reassignment: Kafka omitted the requested partition")
	}
	return partitionError(response.Err)
}
