package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func NewProducer(cfg config.KafkaConfig) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DisableIdempotentWrite(),
		kgo.RecordRetries(0),
		kgo.UnknownTopicRetries(0),
		kgo.RequestRetries(0),
	)
}
