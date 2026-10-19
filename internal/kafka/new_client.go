package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

// NewClient builds the franz-go client for the configured brokers. It does not connect until first use.
func NewClient(cfg config.KafkaConfig) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
}
