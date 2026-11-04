//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/kafka"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestReadTopicMessages_FromOffsetAndTimestamp(t *testing.T) {
	brokers := StartKafka(t)
	client, err := kafka.NewClient(config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Timestamp: base, Key: []byte("first"), Value: []byte("first")},
		{Topic: "orders", Partition: 0, Timestamp: base.Add(time.Second), Key: []byte("second"), Value: []byte("second")},
		{Topic: "orders", Partition: 0, Timestamp: base.Add(2 * time.Second), Key: []byte("third"), Value: []byte("third")},
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	action := topics.NewReadTopicMessages(kafka.NewReadTopicMessages(admin, brokers), 30*time.Second)
	offset := int64(1)
	gotByOffset, err := action.Execute(ctx, "orders", topics.TopicMessagePosition{Partition: 0, Offset: &offset})
	if err != nil {
		t.Fatal(err)
	}
	wantByOffset := []topics.TopicMessage{
		{Partition: 0, Offset: 1, OffsetExact: "1", Timestamp: base.Add(time.Second), Key: []byte("second"), Value: []byte("second"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 2, OffsetExact: "2", Timestamp: base.Add(2 * time.Second), Key: []byte("third"), Value: []byte("third"), Headers: []topics.TopicMessageHeader{}},
	}
	if diff := cmp.Diff(wantByOffset, gotByOffset); diff != "" {
		t.Errorf("offset read mismatch (-want +got):\n%s", diff)
	}

	timestamp := base.Add(time.Second).UnixMilli()
	gotByTimestamp, err := action.Execute(ctx, "orders", topics.TopicMessagePosition{Partition: 0, Timestamp: &timestamp})
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(wantByOffset, gotByTimestamp); diff != "" {
		t.Errorf("timestamp read mismatch (-want +got):\n%s", diff)
	}

	endOffset := int64(3)
	gotAtEnd, err := action.Execute(ctx, "orders", topics.TopicMessagePosition{Partition: 0, Offset: &endOffset})
	if err != nil {
		t.Fatal(err)
	}
	if gotAtEnd == nil || len(gotAtEnd) != 0 {
		t.Errorf("read at end = %#v, want non-nil empty slice", gotAtEnd)
	}

	invalidPartition := int64(0)
	_, err = action.Execute(ctx, "orders", topics.TopicMessagePosition{Partition: 1, Offset: &invalidPartition})
	if !errors.Is(err, topics.ErrInvalidTopicMessagePosition) {
		t.Errorf("invalid partition error = %v, want ErrInvalidTopicMessagePosition", err)
	}
}
