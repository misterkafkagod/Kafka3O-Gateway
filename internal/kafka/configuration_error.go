package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/settings"
)

func configurationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return settings.ErrNotFound
	}
	if errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) {
		return settings.ErrDenied
	}
	var kafkaError *kerr.Error
	if errors.As(err, &kafkaError) && !kafkaError.Retriable {
		return fmt.Errorf("%w: %s", settings.ErrRejected, kafkaError.Message)
	}
	return err
}
