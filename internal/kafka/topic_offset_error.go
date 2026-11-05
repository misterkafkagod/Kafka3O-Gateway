package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type topicConfigDescriber interface {
	DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error)
}

func topicOffsetError(ctx context.Context, admin topicConfigDescriber, name string, offsetErr error) error {
	if !errors.Is(offsetErr, kerr.UnknownTopicOrPartition) && !errors.Is(offsetErr, kerr.UnknownTopicID) {
		return offsetErr
	}
	configs, err := admin.DescribeTopicConfigs(ctx, name)
	if errors.Is(err, kerr.UnknownTopicOrPartition) || errors.Is(err, kerr.UnknownTopicID) {
		return topics.ErrTopicNotFound
	}
	if err != nil {
		return fmt.Errorf("verify topic after offset error: %w", errors.Join(offsetErr, err))
	}
	config, err := configs.On(name, nil)
	if err != nil {
		return fmt.Errorf("verify topic after offset error: missing config result for %q: %w", name, errors.Join(offsetErr, err))
	}
	if errors.Is(config.Err, kerr.UnknownTopicOrPartition) || errors.Is(config.Err, kerr.UnknownTopicID) {
		return topics.ErrTopicNotFound
	}
	if config.Err != nil {
		return fmt.Errorf("verify topic after offset error: %w", errors.Join(offsetErr, config.Err))
	}
	return offsetErr
}
