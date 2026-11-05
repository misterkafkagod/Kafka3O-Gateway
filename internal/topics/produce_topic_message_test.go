package topics

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSendTopicMessage struct {
	calls   int
	topic   string
	request ProduceMessageRequest
	hasDL   bool
	result  ProduceMessageResult
	err     error
}

func (f *fakeSendTopicMessage) Execute(ctx context.Context, topic string, request ProduceMessageRequest) (ProduceMessageResult, error) {
	f.calls++
	f.topic = topic
	f.request = request
	_, f.hasDL = ctx.Deadline()
	return f.result, f.err
}

func TestProduceTopicMessage_ValidatesBeforeSending(t *testing.T) {
	empty, invalid := "", string([]byte{0xff})
	tests := []struct {
		name    string
		topic   string
		request ProduceMessageRequest
	}{
		{"missing value", "orders", ProduceMessageRequest{}},
		{"invalid value", "orders", ProduceMessageRequest{Value: &invalid}},
		{"invalid key", "orders", ProduceMessageRequest{Value: &empty, Key: &invalid}},
		{"missing header key", "orders", ProduceMessageRequest{Value: &empty, Headers: []ProduceMessageHeader{{Value: &empty}}}},
		{"missing header value", "orders", ProduceMessageRequest{Value: &empty, Headers: []ProduceMessageHeader{{Key: &empty}}}},
		{"invalid header", "orders", ProduceMessageRequest{Value: &empty, Headers: []ProduceMessageHeader{{Key: &invalid, Value: &empty}}}},
		{"empty topic", "", ProduceMessageRequest{Value: &empty}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			send := &fakeSendTopicMessage{}
			_, err := NewProduceTopicMessage(send, time.Second).Execute(context.Background(), tt.topic, tt.request)
			if !errors.Is(err, ErrInvalidProduceMessage) || send.calls != 0 {
				t.Fatalf("error = %v, calls = %d", err, send.calls)
			}
		})
	}
}

func TestProduceTopicMessage_PreservesEmptyKeyAndValue(t *testing.T) {
	empty, name, value := "", "source", "test"
	request := ProduceMessageRequest{Value: &empty, Key: &empty, Headers: []ProduceMessageHeader{{Key: &name, Value: &value}}}
	send := &fakeSendTopicMessage{result: ProduceMessageResult{Partition: 3, Offset: "9007199254740993"}}
	got, err := NewProduceTopicMessage(send, time.Second).Execute(context.Background(), "orders", request)
	if err != nil || got != send.result || send.topic != "orders" || send.request.Key == nil || *send.request.Key != "" || !send.hasDL {
		t.Fatalf("result = %#v, error = %v, send = %#v", got, err, send)
	}
}

func TestProduceTopicMessage_PropagatesDeliveryError(t *testing.T) {
	value := "message"
	want := errors.New("delivery unknown")
	send := &fakeSendTopicMessage{err: want}
	_, err := NewProduceTopicMessage(send, time.Second).Execute(context.Background(), "orders", ProduceMessageRequest{Value: &value})
	if !errors.Is(err, want) || send.calls != 1 {
		t.Fatalf("error = %v, calls = %d", err, send.calls)
	}
}
