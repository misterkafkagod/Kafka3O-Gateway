package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type kafkaTopicDeleter interface {
	DeleteTopic(context.Context, string) (kadm.DeleteTopicResponse, error)
}

type deleteKafkaTopic struct {
	admin kafkaTopicDeleter
}

func NewDeleteKafkaTopic(admin kafkaTopicDeleter) topics.DeleteKafkaTopic {
	return deleteKafkaTopic{admin: admin}
}

func (d deleteKafkaTopic) Execute(ctx context.Context, name string) error {
	_, err := d.admin.DeleteTopic(ctx, name)
	if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID) {
		return topics.ErrTopicNotFound
	}
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}
	return nil
}
