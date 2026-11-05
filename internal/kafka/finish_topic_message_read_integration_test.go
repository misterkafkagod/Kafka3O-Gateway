//go:build integration

package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
	"github.com/misterkafkagod/Kafka3O-Gateway/test/integration"
)

func TestFinishTopicMessageRead_RealBroker(t *testing.T) {
	brokers := integration.StartKafka(t)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := admin.CreateTopic(ctx, 2, 1, nil, "snapshot-fetch"); err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	records := []*kgo.Record{
		{Topic: "snapshot-fetch", Partition: 0, Timestamp: base, Value: []byte("skip")},
		{Topic: "snapshot-fetch", Partition: 0, Timestamp: base.Add(time.Second), Key: []byte("key"), Value: []byte("first")},
		{Topic: "snapshot-fetch", Partition: 0, Timestamp: base.Add(2 * time.Second), Value: []byte("second"), Headers: []kgo.RecordHeader{{Key: "source", Value: []byte("integration")}}},
	}
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	details, err := admin.ListTopicsWithInternal(ctx, "snapshot-fetch")
	if err != nil {
		t.Fatal(err)
	}
	detail, ok := details["snapshot-fetch"]
	if !ok || detail.Err != nil {
		t.Fatalf("topic metadata = %#v, present = %t", detail, ok)
	}
	want := []topics.TopicMessage{
		{Partition: 0, Offset: 1, OffsetExact: "1", Timestamp: base.Add(time.Second), Key: []byte("key"), Value: []byte("first"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 2, OffsetExact: "2", Timestamp: base.Add(2 * time.Second), Value: []byte("second"), Headers: []topics.TopicMessageHeader{{Key: "source", Value: []byte("integration")}}},
	}
	read := func(reader *kgo.Client, original kadm.TopicDetail) {
		t.Helper()
		state, err := newTopicMessageReadState("snapshot-fetch", map[int32]int64{0: 1, 1: 0}, map[int32]int64{0: 3, 1: 0}, 50)
		if err != nil {
			t.Fatal(err)
		}
		if err := finishTopicMessageRead(ctx, reader, original, state); err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(want, state.messages); diff != "" {
			t.Fatalf("snapshot messages mismatch (-want +got):\n%s", diff)
		}
	}
	read(client, detail)

	legacy, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.MaxVersions(kversion.V2_8_0()))
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	legacyDetail := detail
	legacyDetail.ID = kadm.TopicID{}
	read(legacy, legacyDetail)

	state, err := newTopicMessageReadState("snapshot-fetch", map[int32]int64{0: 0}, map[int32]int64{0: 4}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := finishTopicMessageRead(ctx, client, detail, state); err == nil || !strings.Contains(err.Error(), "high watermark") {
		t.Fatalf("regressed watermark error = %v", err)
	}
	if len(state.messages) != 0 {
		t.Fatalf("regressed watermark returned partial messages: %#v", state.messages)
	}

	changed := detail
	changed.ID[0] ^= 0xff
	state, err = newTopicMessageReadState("snapshot-fetch", map[int32]int64{0: 0}, map[int32]int64{0: 3}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := finishTopicMessageRead(ctx, client, changed, state); err == nil || !strings.Contains(err.Error(), "changed while reading") {
		t.Fatalf("changed topic ID error = %v", err)
	}

	if _, err := admin.CreateTopic(ctx, 1, 1, nil, "large-snapshot"); err != nil {
		t.Fatal(err)
	}
	values := make([][]byte, 6)
	for i := range values {
		values[i] = make([]byte, 192*1024)
		if _, err := rand.Read(values[i]); err != nil {
			t.Fatal(err)
		}
		if err := client.ProduceSync(ctx, &kgo.Record{Topic: "large-snapshot", Partition: 0, Value: values[i]}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	details, err = admin.ListTopicsWithInternal(ctx, "large-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	largeDetail, ok := details["large-snapshot"]
	if !ok || largeDetail.Err != nil {
		t.Fatalf("large topic metadata = %#v, present = %t", largeDetail, ok)
	}
	decompressor, ok := client.OptValue(kgo.WithDecompressor).(kgo.Decompressor)
	if !ok {
		t.Fatal("client has no decompressor")
	}
	fetcher := topicSnapshotFetcher{
		reader: client, admin: admin, topic: "large-snapshot", originalID: largeDetail.ID, decompressor: decompressor,
	}
	if err := fetcher.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	state, err = newTopicMessageReadState("large-snapshot", map[int32]int64{0: 0}, map[int32]int64{0: int64(len(values))}, 50)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := state.finish(ctx, func(ctx context.Context, partition int32, offset int64) (fetchedTopicPartition, error) {
		calls++
		return fetcher.fetch(ctx, partition, offset)
	}); err != nil {
		t.Fatal(err)
	}
	if calls < 2 || len(state.messages) != len(values) {
		t.Fatalf("large snapshot used %d fetches and returned %d messages; want at least 2 fetches and %d messages", calls, len(state.messages), len(values))
	}
	for i, message := range state.messages {
		if message.Offset != int64(i) || !bytes.Equal(message.Value, values[i]) {
			t.Fatalf("large snapshot message %d has offset %d or an unexpected value", i, message.Offset)
		}
	}
}
