//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestGetTopicDetail_ReturnsPartitionsOffsetsAndConfig(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	policy := "compact"
	if _, err := admin.CreateTopic(ctx, 2, 1, map[string]*string{"cleanup.policy": &policy}, "orders"); err != nil {
		t.Fatal(err)
	}
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Key: []byte("key-zero"), Value: []byte("partition-zero")},
		{Topic: "orders", Partition: 1, Key: []byte("key-one"), Value: []byte("partition-one")},
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	got, err := topics.NewGetTopicDetail(kafka.NewDescribeTopic(admin), 30*time.Second).Execute(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "orders" {
		t.Fatalf("name = %q, want orders", got.Name)
	}
	if len(got.Partitions) != 2 {
		t.Fatalf("partitions = %+v, want two partitions", got.Partitions)
	}
	for i, partition := range got.Partitions {
		if partition.ID != int32(i) {
			t.Errorf("partition[%d].ID = %d, want %d", i, partition.ID, i)
		}
		if partition.StartOffset != 0 || partition.EndOffset != 1 {
			t.Errorf("partition[%d] offsets = [%d,%d), want [0,1)", i, partition.StartOffset, partition.EndOffset)
		}
	}
	for _, entry := range got.Config {
		if entry.Name == "cleanup.policy" {
			if entry.Value != policy || entry.Sensitive {
				t.Errorf("cleanup.policy = %+v, want value %q and not sensitive", entry, policy)
			}
			return
		}
	}
	t.Fatal("cleanup.policy missing from topic config")
}
