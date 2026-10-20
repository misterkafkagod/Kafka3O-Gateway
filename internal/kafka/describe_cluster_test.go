package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func TestDescribeCluster_ReturnsErrorWhenKafkaIsUnreachable(t *testing.T) {
	client, err := NewClient(config.KafkaConfig{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()

	_, err = NewDescribeCluster(kadm.NewClient(client)).Execute(ctx)

	if err == nil {
		t.Fatal("expected an error for an unreachable broker")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("returned after %v, want within 2s", elapsed)
	}
}
