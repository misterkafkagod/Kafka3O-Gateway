//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
)

func TestDescribeCluster_ReturnsClusterIDAndOneBroker(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewClient(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := kafka.NewDescribeCluster(kadm.NewClient(client)).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if got.ID != kafkaClusterID {
		t.Errorf("cluster ID = %q, want %q", got.ID, kafkaClusterID)
	}
	if got.ControllerID != 1 {
		t.Errorf("controller ID = %d, want 1", got.ControllerID)
	}
	if len(got.Brokers) != 1 || got.Brokers[0].ID != 1 {
		t.Errorf("brokers = %+v, want exactly broker 1", got.Brokers)
	}
}
