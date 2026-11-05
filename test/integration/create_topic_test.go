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

func TestCreateTopic_CreatesAndDetectsConflict(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewProducer(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	action := topics.NewCreateTopic(kafka.NewCreateKafkaTopic(admin), 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	request := topics.CreateTopicRequest{Name: "created-orders", Partitions: 2, ReplicationFactor: 1}
	if err := action.Execute(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := action.Execute(ctx, request); !errors.Is(err, topics.ErrTopicAlreadyExists) {
		t.Fatalf("duplicate error = %v", err)
	}
	details, err := admin.ListTopicsWithInternal(ctx, request.Name)
	if err != nil {
		t.Fatal(err)
	}
	if detail, ok := details[request.Name]; !ok || detail.Err != nil || len(detail.Partitions) != 2 {
		t.Fatalf("created topic metadata = %#v", details)
	}
}
