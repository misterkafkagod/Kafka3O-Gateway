package topics

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeFetchTopicMessagesFromPosition struct {
	messages []TopicMessage
	err      error
	position TopicMessagePosition
	called   bool
	deadline bool
}

func (f *fakeFetchTopicMessagesFromPosition) Execute(ctx context.Context, _ string, position TopicMessagePosition) ([]TopicMessage, error) {
	f.called = true
	f.position = position
	_, f.deadline = ctx.Deadline()
	return f.messages, f.err
}

func TestReadTopicMessages_ValidatesAndPassesOffsetOrTimestamp(t *testing.T) {
	offset := int64(4)
	timestamp := int64(1700000000123)
	tests := []struct {
		name     string
		position TopicMessagePosition
	}{
		{name: "offset", position: TopicMessagePosition{Partition: 2, Offset: &offset}},
		{name: "timestamp", position: TopicMessagePosition{Partition: 1, Timestamp: &timestamp}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetch := &fakeFetchTopicMessagesFromPosition{}
			_, err := NewReadTopicMessages(fetch, time.Second).Execute(context.Background(), "orders", tt.position)
			if err != nil {
				t.Fatal(err)
			}
			if !fetch.called {
				t.Fatal("fetch was not called")
			}
			if fetch.position.Partition != tt.position.Partition || fetch.position.Offset != tt.position.Offset || fetch.position.Timestamp != tt.position.Timestamp {
				t.Errorf("position = %+v, want %+v", fetch.position, tt.position)
			}
			if !fetch.deadline {
				t.Error("fetch context has no timeout deadline")
			}
		})
	}
}

func TestReadTopicMessages_RejectsInvalidPositionsBeforeFetching(t *testing.T) {
	negative := int64(-1)
	zero := int64(0)
	tests := []struct {
		name     string
		position TopicMessagePosition
	}{
		{name: "negative partition", position: TopicMessagePosition{Partition: -1, Offset: &zero}},
		{name: "missing position", position: TopicMessagePosition{Partition: 0}},
		{name: "both positions", position: TopicMessagePosition{Partition: 0, Offset: &zero, Timestamp: &zero}},
		{name: "negative offset", position: TopicMessagePosition{Partition: 0, Offset: &negative}},
		{name: "negative timestamp", position: TopicMessagePosition{Partition: 0, Timestamp: &negative}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetch := &fakeFetchTopicMessagesFromPosition{}
			_, err := NewReadTopicMessages(fetch, time.Second).Execute(context.Background(), "orders", tt.position)
			if !errors.Is(err, ErrInvalidTopicMessagePosition) {
				t.Fatalf("error = %v, want ErrInvalidTopicMessagePosition", err)
			}
			if fetch.called {
				t.Error("fetch was called for an invalid position")
			}
		})
	}
}

func TestReadTopicMessages_SortsAndCapsAtFifty(t *testing.T) {
	messages := make([]TopicMessage, 60)
	for i := range messages {
		messages[i] = TopicMessage{Offset: int64(59 - i)}
	}
	got, err := NewReadTopicMessages(&fakeFetchTopicMessagesFromPosition{messages: messages}, time.Second).
		Execute(context.Background(), "orders", TopicMessagePosition{Partition: 0, Offset: new(int64)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 {
		t.Fatalf("message count = %d, want 50", len(got))
	}
	for i, message := range got {
		if message.Offset != int64(i) {
			t.Errorf("offset at index %d = %d, want %d", i, message.Offset, i)
		}
	}
}

func TestReadTopicMessages_ReturnsEmptyArray(t *testing.T) {
	got, err := NewReadTopicMessages(&fakeFetchTopicMessagesFromPosition{}, time.Second).
		Execute(context.Background(), "orders", TopicMessagePosition{Partition: 0, Offset: new(int64)})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("messages = %#v, want non-nil empty slice", got)
	}
}

func TestReadTopicMessages_PropagatesFetchError(t *testing.T) {
	want := errors.New("kafka unavailable")
	_, err := NewReadTopicMessages(&fakeFetchTopicMessagesFromPosition{err: want}, time.Second).
		Execute(context.Background(), "orders", TopicMessagePosition{Partition: 0, Offset: new(int64)})
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want %v", err, want)
	}
}
