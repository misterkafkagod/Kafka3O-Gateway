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

func TestLiveMetadata_ReturnsErrorsAfterBrokerStops(t *testing.T) {
	brokers, container := StartKafkaWithContainer(t)
	client, err := kafka.NewClient(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(client).CreateTopic(ctx, 1, 1, nil, "live-metadata"); err != nil {
		t.Fatal(err)
	}
	describe := kafka.NewDescribeCluster(client)
	list := kafka.NewListTopicMetadata(client)
	if _, err := describe.Execute(ctx); err != nil {
		t.Fatalf("initial cluster metadata: %v", err)
	}
	initialTopics, err := list.Execute(ctx)
	if err != nil {
		t.Fatalf("initial topic metadata: %v", err)
	}
	found := false
	for _, topic := range initialTopics {
		if topic.Name == "live-metadata" {
			found = true
		}
	}
	if !found {
		t.Fatalf("created topic missing from initial metadata: %+v", initialTopics)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopCancel()
	grace := time.Second
	if err := container.Stop(stopCtx, &grace); err != nil {
		t.Fatalf("stop Kafka: %v", err)
	}

	clusterCtx, clusterCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer clusterCancel()
	if _, err := describe.Execute(clusterCtx); err == nil {
		t.Error("cluster metadata succeeded after Kafka stopped")
	}
	topicsCtx, topicsCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer topicsCancel()
	if _, err := list.Execute(topicsCtx); err == nil {
		t.Error("topic metadata succeeded after Kafka stopped")
	}
}
