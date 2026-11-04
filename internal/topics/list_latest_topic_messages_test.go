package topics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type fakeFetchTopicMessages struct {
	messages []TopicMessage
	err      error
	topic    string
	deadline time.Time
	hasDL    bool
}

func (f *fakeFetchTopicMessages) Execute(ctx context.Context, topic string) ([]TopicMessage, error) {
	f.topic = topic
	f.deadline, f.hasDL = ctx.Deadline()
	return f.messages, f.err
}

func TestListLatestTopicMessages_SortsAndCapsAt50(t *testing.T) {
	messages := make([]TopicMessage, 60)
	for i := range messages {
		messages[i] = TopicMessage{Offset: int64(i), Timestamp: time.UnixMilli(int64(i))}
	}
	fetch := &fakeFetchTopicMessages{messages: messages}

	got, err := NewListLatestTopicMessages(fetch, time.Second).Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 {
		t.Fatalf("got %d messages, want 50", len(got))
	}
	for i, message := range got {
		wantOffset := int64(59 - i)
		if message.Offset != wantOffset {
			t.Errorf("message[%d].Offset = %d, want %d", i, message.Offset, wantOffset)
		}
	}
	if fetch.topic != "orders" {
		t.Errorf("requested topic = %q, want orders", fetch.topic)
	}
}

func TestListLatestTopicMessages_UsesDeterministicTies(t *testing.T) {
	timestamp := time.UnixMilli(100)
	fetch := &fakeFetchTopicMessages{messages: []TopicMessage{
		{Partition: 1, Offset: 2, Timestamp: timestamp},
		{Partition: 0, Offset: 4, Timestamp: timestamp},
		{Partition: 0, Offset: 5, Timestamp: timestamp},
	}}

	got, err := NewListLatestTopicMessages(fetch, time.Second).Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	want := []TopicMessage{
		{Partition: 0, Offset: 5, Timestamp: timestamp},
		{Partition: 0, Offset: 4, Timestamp: timestamp},
		{Partition: 1, Offset: 2, Timestamp: timestamp},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("messages mismatch (-want +got):\n%s", diff)
	}
}

func TestListLatestTopicMessages_ReturnsEmptyArray(t *testing.T) {
	got, err := NewListLatestTopicMessages(&fakeFetchTopicMessages{}, time.Second).Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("messages = %#v, want empty non-nil slice", got)
	}
}

func TestListLatestTopicMessages_PassesErrorsThrough(t *testing.T) {
	want := errors.New("fetch failed")

	_, err := NewListLatestTopicMessages(&fakeFetchTopicMessages{err: want}, time.Second).Execute(context.Background(), "orders")
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}

func TestListLatestTopicMessages_PassesConfiguredDeadline(t *testing.T) {
	fetch := &fakeFetchTopicMessages{}
	start := time.Now()

	_, err := NewListLatestTopicMessages(fetch, 2*time.Second).Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	end := time.Now()

	if !fetch.hasDL {
		t.Fatal("fetch port received no deadline")
	}
	if fetch.deadline.Before(start.Add(2*time.Second)) || fetch.deadline.After(end.Add(2*time.Second)) {
		t.Errorf("deadline %v not within [start+2s, end+2s]", fetch.deadline)
	}
}
