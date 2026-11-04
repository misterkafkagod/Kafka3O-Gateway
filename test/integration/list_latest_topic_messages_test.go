//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestListLatestTopicMessages_ReturnsNewestAcrossPartitions(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewClient(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 2, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Timestamp: base, Key: []byte("first-0"), Value: []byte("first-0")},
		{Topic: "orders", Partition: 1, Timestamp: base.Add(3 * time.Second), Key: []byte("last"), Value: []byte("last"), Headers: []kgo.RecordHeader{{Key: "source", Value: []byte("integration")}}},
		{Topic: "orders", Partition: 0, Timestamp: base.Add(time.Second), Key: []byte("second-0"), Value: []byte("second-0")},
		{Topic: "orders", Partition: 1, Timestamp: base.Add(2 * time.Second), Key: []byte("second-1"), Value: []byte("second-1")},
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	action := topics.NewListLatestTopicMessages(kafka.NewListLatestTopicMessages(admin, brokers), 30*time.Second)
	got, err := action.Execute(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	want := []topics.TopicMessage{
		{Partition: 1, Offset: 0, Timestamp: base.Add(3 * time.Second), Key: []byte("last"), Value: []byte("last"), Headers: []topics.TopicMessageHeader{{Key: "source", Value: []byte("integration")}}},
		{Partition: 1, Offset: 1, Timestamp: base.Add(2 * time.Second), Key: []byte("second-1"), Value: []byte("second-1"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 1, Timestamp: base.Add(time.Second), Key: []byte("second-0"), Value: []byte("second-0"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 0, Timestamp: base, Key: []byte("first-0"), Value: []byte("first-0"), Headers: []topics.TopicMessageHeader{}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%s", diff)
	}
}
