package kafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

// NewClient builds the franz-go client for the configured brokers. It does not connect until first use.
func NewClient(cfg config.KafkaConfig) (*kgo.Client, error) {
	opts, err := newKafkaClientOptions(cfg, os.ReadFile)
	if err != nil {
		return nil, err
	}
	return kgo.NewClient(opts...)
}
