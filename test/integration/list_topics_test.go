//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestListTopics_CountsMessages(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewClient(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 6, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	records := make([]*kgo.Record, 0, 10)
	for i := range 10 {
		records = append(records, &kgo.Record{Topic: "orders", Value: fmt.Appendf(nil, "%d", i)})
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	listTopics := topics.NewListTopics(kafka.NewListTopicMetadata(admin), kafka.NewCountTopicMessages(admin), 30*time.Second)
	got, err := listTopics.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var orders *topics.Topic
	for i := range got {
		if got[i].Name == "orders" {
			orders = &got[i]
		}
	}
	if orders == nil {
		t.Fatalf("orders not listed in %+v", got)
	}
	want := topics.Topic{Name: "orders", Partitions: 6, ReplicationFactor: 1, ApproxMessages: 10}
	if *orders != want {
		t.Errorf("orders = %+v, want %+v", *orders, want)
	}
}
