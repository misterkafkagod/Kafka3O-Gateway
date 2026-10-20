package kafka

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func TestNewClient_BuildsWithoutConnecting(t *testing.T) {
	// Port 1 has no broker: building must still succeed because franz-go connects lazily.
	client, err := NewClient(config.KafkaConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.Close()
}
