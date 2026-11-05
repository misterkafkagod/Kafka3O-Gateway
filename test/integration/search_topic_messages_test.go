//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestSearchTopicMessages_ScansAtMost500OffsetsAcrossPartitions(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 2, 1, nil, "search-orders"); err != nil {
		t.Fatal(err)
	}
	records := make([]*kgo.Record, 0, 600)
	for i := 0; i < 600; i++ {
		partition := int32(0)
		if i >= 400 {
			partition = 1
		}
		records = append(records, &kgo.Record{Topic: "search-orders", Partition: partition, Value: []byte(fmt.Sprintf("match-%d", i))})
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	action := topics.NewSearchTopicMessages(kafka.NewFetchRecentTopicMessages(admin, client), 30*time.Second)
	got, err := action.Execute(ctx, "search-orders", "match")
	if err != nil {
		t.Fatal(err)
	}
	if got.Scanned != 500 || len(got.Messages) != 500 || !got.Truncated {
		t.Fatalf("scanned = %d, matches = %d, truncated = %v; want 500, 500, true", got.Scanned, len(got.Messages), got.Truncated)
	}
	counts := make(map[int32]int)
	for _, message := range got.Messages {
		counts[message.Partition]++
		if message.Partition == 0 && message.Offset < 100 {
			t.Errorf("partition 0 offset %d is outside the selected window", message.Offset)
		}
	}
	if counts[0] != 300 || counts[1] != 200 {
		t.Errorf("partition counts = %v, want 300 and 200", counts)
	}
}
