package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type kafkaTopicCreator interface {
	CreateTopic(context.Context, int32, int16, map[string]*string, string) (kadm.CreateTopicResponse, error)
}

type createKafkaTopic struct {
	admin kafkaTopicCreator
}

func NewCreateKafkaTopic(admin kafkaTopicCreator) topics.CreateKafkaTopic {
	return createKafkaTopic{admin: admin}
}

func (c createKafkaTopic) Execute(ctx context.Context, request topics.CreateTopicRequest) error {
	_, err := c.admin.CreateTopic(ctx, request.Partitions, request.ReplicationFactor, nil, request.Name)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return topics.ErrTopicAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return nil
}
