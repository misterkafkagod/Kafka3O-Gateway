package kafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func NewProducer(cfg config.KafkaConfig) (*kgo.Client, error) {
	opts, err := newKafkaClientOptions(cfg, os.ReadFile)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.DisableIdempotentWrite(),
		kgo.RecordRetries(0),
		kgo.UnknownTopicRetries(0),
		kgo.RequestRetries(0),
	)
	return kgo.NewClient(opts...)
}
