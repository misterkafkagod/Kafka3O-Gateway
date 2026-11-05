//go:build integration

package kafka

import (
	"bytes"
	"context"
	"crypto/rand"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/misterkafkagod/Kafka3O-Gateway/test/integration"
)

func TestFinishTopicMessageRead_CompactedTopic(t *testing.T) {
	brokers := integration.StartKafka(t)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	admin := kadm.NewClient(producer)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const topic = "compacted-snapshot"
	configs := map[string]*string{
		"cleanup.policy":            kmsg.StringPtr("compact"),
		"segment.bytes":             kmsg.StringPtr("1048576"),
		"segment.ms":                kmsg.StringPtr("1000"),
		"min.cleanable.dirty.ratio": kmsg.StringPtr("0.01"),
	}
	if _, err := admin.CreateTopic(ctx, 1, 1, configs, topic); err != nil {
		t.Fatal(err)
	}
	records := make([]*kgo.Record, 0, 74)
	records = append(records, &kgo.Record{Topic: topic, Key: []byte("anchor"), Value: []byte("anchor")})
	for i := 0; i < 72; i++ {
		value := make([]byte, 512)
		if _, err := rand.Read(value); err != nil {
			t.Fatal(err)
		}
		records = append(records, &kgo.Record{Topic: topic, Key: []byte("duplicate"), Value: value})
	}
	for _, record := range records {
		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-time.After(1500 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	last := &kgo.Record{Topic: topic, Key: []byte("duplicate"), Value: []byte("last")}
	if err := producer.ProduceSync(ctx, last).FirstErr(); err != nil {
		t.Fatal(err)
	}
	records = append(records, last)
	if last.Offset != int64(len(records)-1) {
		t.Fatalf("last offset = %d, want %d", last.Offset, len(records)-1)
	}

	read := func() ([]*kgo.Record, error) {
		reader, err := kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var got []*kgo.Record
		for {
			fetches := reader.PollFetches(readCtx)
			if err := fetches.Err(); err != nil {
				return nil, err
			}
			for _, record := range fetches.Records() {
				got = append(got, record)
				if record.Offset == last.Offset {
					return got, nil
				}
			}
		}
	}
	offsets := func(records []*kgo.Record) []int64 {
		got := make([]int64, len(records))
		for i, record := range records {
			got[i] = record.Offset
		}
		return got
	}
	var before []*kgo.Record
	for {
		before, err = read()
		if err != nil {
			t.Fatalf("read topic while waiting for compaction: %v", err)
		}
		if len(before) < 10 {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			t.Fatalf("Kafka did not compact %d records: %v", len(records), ctx.Err())
		}
	}
	if len(before) == 0 || before[0].Offset != 0 || before[len(before)-1].Offset != last.Offset {
		t.Fatalf("compacted offsets = %v; want the retained anchor and last record", offsets(before))
	}
	details, err := admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		t.Fatalf("compacted topic metadata = %#v, present = %t", detail, ok)
	}
	for {
		state, err := newTopicMessageReadState(topic, map[int32]int64{0: 0}, map[int32]int64{0: last.Offset + 1}, 50)
		if err != nil {
			t.Fatal(err)
		}
		if err := finishTopicMessageRead(ctx, producer, detail, state); err != nil {
			t.Fatal(err)
		}
		after, err := read()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(offsets(before), offsets(after)) {
			before = after
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			continue
		}
		if len(state.messages) != len(before) {
			t.Fatalf("snapshot returned %d messages; broker returned offsets %v", len(state.messages), offsets(before))
		}
		for i, message := range state.messages {
			if message.Offset != before[i].Offset || !bytes.Equal(message.Key, before[i].Key) || !bytes.Equal(message.Value, before[i].Value) {
				t.Fatalf("snapshot message %d does not match compacted broker offset %d", i, before[i].Offset)
			}
		}
		break
	}
}
