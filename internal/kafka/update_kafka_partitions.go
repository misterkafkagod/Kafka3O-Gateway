package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

type partitionCountAdmin interface {
	ValidateUpdatePartitions(context.Context, int, ...string) (kadm.CreatePartitionsResponses, error)
	UpdatePartitions(context.Context, int, ...string) (kadm.CreatePartitionsResponses, error)
}

type updateKafkaPartitions struct {
	admin partitionCountAdmin
}

func NewUpdateKafkaPartitions(admin partitionCountAdmin) partitions.UpdateKafkaPartitions {
	return updateKafkaPartitions{admin: admin}
}

func (u updateKafkaPartitions) Execute(ctx context.Context, topic string, count int32, validateOnly bool) error {
	var responses kadm.CreatePartitionsResponses
	var err error
	if validateOnly {
		responses, err = u.admin.ValidateUpdatePartitions(ctx, int(count), topic)
	} else {
		responses, err = u.admin.UpdatePartitions(ctx, int(count), topic)
	}
	if err != nil {
		return fmt.Errorf("update partitions: %w", partitionError(err))
	}
	response, err := responses.On(topic, nil)
	if err != nil {
		return fmt.Errorf("update partitions response: %w", partitionError(err))
	}
	return partitionError(response.Err)
}
