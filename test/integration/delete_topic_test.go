//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestDeleteTopic_AcceptsAndEventuallyRemovesTopic(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewProducer(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, "delete-orders"); err != nil {
		t.Fatal(err)
	}
	metadata := kafka.NewListTopicMetadata(client)
	waitForTopicPresence(t, ctx, metadata, "delete-orders", true)
	action := topics.NewDeleteTopic(kafka.NewDeleteKafkaTopic(admin), 30*time.Second)
	if err := action.Execute(ctx, "delete-orders"); err != nil {
		t.Fatal(err)
	}
	waitForTopicPresence(t, ctx, metadata, "delete-orders", false)
	if err := action.Execute(ctx, "delete-orders"); !errors.Is(err, topics.ErrTopicNotFound) {
		t.Fatalf("missing error = %v", err)
	}
}

func waitForTopicPresence(t *testing.T, ctx context.Context, metadata topics.ListTopicMetadata, name string, present bool) {
	t.Helper()
	for {
		entries, err := metadata.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, entry := range entries {
			found = found || entry.Name == name
		}
		if found == present {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic %q presence did not become %v: %v", name, present, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
