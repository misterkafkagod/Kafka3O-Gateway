package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

type fakeSyncProducer struct {
	record *kgo.Record
	err    error
}

func (f *fakeSyncProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.record = records[0]
	f.record.Partition = 2
	f.record.Offset = 9007199254740993
	return kgo.ProduceResults{{Record: f.record, Err: f.err}}
}

func TestSendTopicMessage_MapsUTF8AndExactOffset(t *testing.T) {
	value, key, headerKey, headerValue := "café", "", "source", "λ"
	producer := &fakeSyncProducer{}
	request := topics.ProduceMessageRequest{Value: &value, Key: &key, Headers: []topics.ProduceMessageHeader{{Key: &headerKey, Value: &headerValue}}}
	got, err := NewSendTopicMessage(producer).Execute(context.Background(), "orders", request)
	if err != nil {
		t.Fatal(err)
	}
	if got != (topics.ProduceMessageResult{Partition: 2, Offset: "9007199254740993"}) {
		t.Errorf("result = %#v", got)
	}
	if producer.record.Topic != "orders" || producer.record.Key == nil || len(producer.record.Key) != 0 ||
		string(producer.record.Value) != value || !cmp.Equal(producer.record.Headers, []kgo.RecordHeader{{Key: headerKey, Value: []byte(headerValue)}}) {
		t.Errorf("record = %#v", producer.record)
	}
}

func TestSendTopicMessage_DistinguishesMissingKeyAndEmptyValue(t *testing.T) {
	value := ""
	producer := &fakeSyncProducer{}
	_, err := NewSendTopicMessage(producer).Execute(context.Background(), "orders", topics.ProduceMessageRequest{Value: &value})
	if err != nil || producer.record.Key != nil || producer.record.Value == nil || len(producer.record.Value) != 0 {
		t.Fatalf("error = %v, record = %#v", err, producer.record)
	}
}

func TestSendTopicMessage_PropagatesProducerError(t *testing.T) {
	value := "message"
	want := errors.New("response lost")
	producer := &fakeSyncProducer{err: want}
	_, err := NewSendTopicMessage(producer).Execute(context.Background(), "orders", topics.ProduceMessageRequest{Value: &value})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}
