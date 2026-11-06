package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/partitions"
)

func partitionError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return partitions.ErrNotFound
	}
	if errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) {
		return partitions.ErrDenied
	}
	var kafkaError *kerr.Error
	if errors.As(err, &kafkaError) && !kafkaError.Retriable {
		return fmt.Errorf("%w: %s", partitions.ErrRejected, kafkaError.Message)
	}
	return err
}
