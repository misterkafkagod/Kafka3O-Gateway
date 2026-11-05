package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func TestNewProducer_DisablesAutomaticRetries(t *testing.T) {
	client, err := NewProducer(config.KafkaConfig{Brokers: []string{"localhost:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.OptValue(kgo.RecordRetries) != int64(0) ||
		client.OptValue(kgo.UnknownTopicRetries) != int64(0) ||
		client.OptValue(kgo.RequestRetries) != int64(0) ||
		client.OptValue(kgo.DisableIdempotentWrite) != true {
		t.Fatalf("unexpected producer options: record=%v unknown=%v request=%v disabled-idempotency=%v",
			client.OptValue(kgo.RecordRetries), client.OptValue(kgo.UnknownTopicRetries),
			client.OptValue(kgo.RequestRetries), client.OptValue(kgo.DisableIdempotentWrite))
	}
}
