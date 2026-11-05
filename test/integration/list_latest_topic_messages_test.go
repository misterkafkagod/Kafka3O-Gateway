//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestListLatestTopicMessages_ReturnsNewestAcrossPartitions(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
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
		{Partition: 1, Offset: 0, OffsetExact: "0", Timestamp: base.Add(3 * time.Second), Key: []byte("last"), Value: []byte("last"), Headers: []topics.TopicMessageHeader{{Key: "source", Value: []byte("integration")}}},
		{Partition: 1, Offset: 1, OffsetExact: "1", Timestamp: base.Add(2 * time.Second), Key: []byte("second-1"), Value: []byte("second-1"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 1, OffsetExact: "1", Timestamp: base.Add(time.Second), Key: []byte("second-0"), Value: []byte("second-0"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 0, OffsetExact: "0", Timestamp: base, Key: []byte("first-0"), Value: []byte("first-0"), Headers: []topics.TopicMessageHeader{}},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%s", diff)
	}
}

func TestTopicMessageReads_EmptyPartitionsAndTransactionMarkers(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 3, 1, nil, "message-boundaries"); err != nil {
		t.Fatal(err)
	}

	latest := topics.NewListLatestTopicMessages(kafka.NewListLatestTopicMessages(admin, brokers), 5*time.Second)
	positioned := topics.NewReadTopicMessages(kafka.NewReadTopicMessages(admin, brokers), 5*time.Second)
	search := topics.NewSearchTopicMessages(kafka.NewFetchRecentTopicMessages(admin, brokers), 5*time.Second)
	got, err := latest.Execute(ctx, "message-boundaries")
	if err != nil {
		t.Fatalf("read empty topic: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty topic messages = %#v, want a non-nil empty slice", got)
	}
	emptySearch, err := search.Execute(ctx, "message-boundaries", "match")
	if err != nil {
		t.Fatalf("search empty topic: %v", err)
	}
	if emptySearch.Messages == nil || len(emptySearch.Messages) != 0 || emptySearch.Scanned != 0 || emptySearch.Truncated {
		t.Fatalf("empty search = %#v", emptySearch)
	}

	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	first := &kgo.Record{Topic: "message-boundaries", Partition: 0, Timestamp: base, Value: []byte("match-first")}
	if err := client.ProduceSync(ctx, first).FirstErr(); err != nil {
		t.Fatal(err)
	}
	got, err = latest.Execute(ctx, "message-boundaries")
	if err != nil {
		t.Fatalf("read topic with empty partitions: %v", err)
	}
	wantFirst := []topics.TopicMessage{
		{Partition: 0, Offset: 0, OffsetExact: "0", Timestamp: base, Value: []byte("match-first"), Headers: []topics.TopicMessageHeader{}},
	}
	if diff := cmp.Diff(wantFirst, got); diff != "" {
		t.Fatalf("mixed partition messages mismatch (-want +got):\n%s", diff)
	}
	zero := int64(0)
	got, err = positioned.Execute(ctx, "message-boundaries", topics.TopicMessagePosition{Partition: 1, Offset: &zero})
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("read empty partition = %#v, error = %v", got, err)
	}

	transactional, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.TransactionalID("message-boundaries-integration"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer transactional.Close()
	if err := transactional.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	transactionRecord := &kgo.Record{
		Topic: "message-boundaries", Partition: 2, Timestamp: base.Add(time.Second), Value: []byte("match-transaction"),
	}
	if err := transactional.ProduceSync(ctx, transactionRecord).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := transactional.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	ends, err := admin.ListEndOffsets(ctx, "message-boundaries")
	if err != nil {
		t.Fatal(err)
	}
	if err := ends.Error(); err != nil {
		t.Fatal(err)
	}
	end, ok := ends.Lookup("message-boundaries", 2)
	if !ok || end.Err != nil || end.Offset != 2 {
		t.Fatalf("transaction partition end = %#v, present = %t; want data and a control marker", end, ok)
	}

	wantTransaction := topics.TopicMessage{
		Partition: 2, Offset: 0, OffsetExact: "0", Timestamp: base.Add(time.Second),
		Value: []byte("match-transaction"), Headers: []topics.TopicMessageHeader{},
	}
	got, err = latest.Execute(ctx, "message-boundaries")
	if err != nil {
		t.Fatalf("read topic ending in a transaction marker: %v", err)
	}
	if diff := cmp.Diff([]topics.TopicMessage{wantTransaction, wantFirst[0]}, got); diff != "" {
		t.Fatalf("transaction messages mismatch (-want +got):\n%s", diff)
	}
	got, err = positioned.Execute(ctx, "message-boundaries", topics.TopicMessagePosition{Partition: 2, Offset: &zero})
	if err != nil {
		t.Fatalf("read transaction partition: %v", err)
	}
	if diff := cmp.Diff([]topics.TopicMessage{wantTransaction}, got); diff != "" {
		t.Fatalf("positioned messages mismatch (-want +got):\n%s", diff)
	}
	markerOffset := int64(1)
	got, err = positioned.Execute(ctx, "message-boundaries", topics.TopicMessagePosition{Partition: 2, Offset: &markerOffset})
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("read transaction marker = %#v, error = %v", got, err)
	}
	found, err := search.Execute(ctx, "message-boundaries", "match")
	if err != nil {
		t.Fatalf("search transaction topic: %v", err)
	}
	if found.Scanned != 2 || found.Truncated {
		t.Fatalf("search scanned = %d, truncated = %t; want 2, false", found.Scanned, found.Truncated)
	}
	if diff := cmp.Diff([]topics.TopicMessage{wantTransaction, wantFirst[0]}, found.Messages); diff != "" {
		t.Fatalf("search messages mismatch (-want +got):\n%s", diff)
	}
}
