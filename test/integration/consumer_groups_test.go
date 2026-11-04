//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
)

func TestConsumerGroups_ClassicAndModernLag(t *testing.T) {
	brokers := StartKafka(t)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	admin := kadm.NewClient(producer)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 2, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: "orders", Partition: 0, Value: []byte("first")},
		&kgo.Record{Topic: "orders", Partition: 0, Value: []byte("second")},
		&kgo.Record{Topic: "orders", Partition: 0, Value: []byte("third")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	for _, protocol := range []string{"classic", "consumer"} {
		options := []kgo.Opt{
			kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(protocol), kgo.ConsumeTopics("orders"),
			kgo.DisableAutoCommit(), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		}
		if protocol == "consumer" {
			options = append(options, kgo.ServerSideBalancer())
		}
		consumer, err := kgo.NewClient(options...)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		var first *kgo.Record
		for first == nil {
			fetches := consumer.PollFetches(ctx)
			if failures := fetches.Errors(); len(failures) > 0 {
				t.Fatalf("%s fetch: %v", protocol, failures)
			}
			for _, record := range fetches.Records() {
				if record.Partition == 0 && record.Offset == 0 {
					first = record
				}
			}
		}
		if err := consumer.CommitRecords(ctx, first); err != nil {
			t.Fatalf("%s commit: %v", protocol, err)
		}
	}

	got, err := groups.NewListConsumerGroups(kafka.NewFetchConsumerGroups(admin), 30*time.Second).Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("groups = %#v, want classic and consumer", got)
	}
	for _, group := range got {
		if group.ID != group.Protocol || group.State != "Stable" || len(group.Members) != 1 ||
			len(group.Topics) != 1 || group.Topics[0] != "orders" || len(group.Partitions) != 2 {
			t.Fatalf("group = %#v", group)
		}
		committed, missing := group.Partitions[0], group.Partitions[1]
		if committed.Partition != 0 || committed.Status != "ok" || committed.CommittedOffset == nil ||
			*committed.CommittedOffset != "1" || committed.Lag == nil || *committed.Lag != "2" {
			t.Errorf("%s committed partition = %#v", group.ID, committed)
		}
		if missing.Partition != 1 || missing.Status != "no_commit" || missing.Lag != nil {
			t.Errorf("%s missing partition = %#v", group.ID, missing)
		}
	}
}
