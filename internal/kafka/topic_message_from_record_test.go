package kafka

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestTopicMessageFromRecord_MapsBytesAndMetadata(t *testing.T) {
	record := &kgo.Record{
		Partition: 2,
		Offset:    19,
		Timestamp: time.Unix(1_800_000_000, 123_000_000),
		Key:       []byte("order-19"),
		Value:     []byte(`{"status":"created"}`),
		Headers: []kgo.RecordHeader{
			{Key: "content-type", Value: []byte("application/json")},
		},
	}

	got := topicMessageFromRecord(record)
	want := topics.TopicMessage{
		Partition:   2,
		Offset:      19,
		OffsetExact: "19",
		Timestamp:   record.Timestamp,
		Key:         []byte("order-19"),
		Value:       []byte(`{"status":"created"}`),
		Headers: []topics.TopicMessageHeader{
			{Key: "content-type", Value: []byte("application/json")},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("message mismatch (-want +got):\n%s", diff)
	}
}

func TestTopicMessageFromRecord_PreservesOffsetsBeyondJavaScriptPrecision(t *testing.T) {
	record := &kgo.Record{Offset: 9007199254740993}
	got := topicMessageFromRecord(record)
	if got.Offset != record.Offset || got.OffsetExact != "9007199254740993" {
		t.Errorf("offset = %d, exact = %q", got.Offset, got.OffsetExact)
	}
}

func TestTopicMessageFromRecord_ReturnsEmptyHeadersWhenAbsent(t *testing.T) {
	got := topicMessageFromRecord(&kgo.Record{})

	if got.Headers == nil || len(got.Headers) != 0 {
		t.Errorf("headers = %#v, want empty non-nil slice", got.Headers)
	}
}
