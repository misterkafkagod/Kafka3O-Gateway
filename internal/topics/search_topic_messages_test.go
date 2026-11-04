package topics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type fakeRecentTopicMessages struct {
	window   TopicMessageWindow
	err      error
	topic    string
	deadline time.Time
	calls    int
}

func (f *fakeRecentTopicMessages) Execute(ctx context.Context, topic string) (TopicMessageWindow, error) {
	f.topic = topic
	f.deadline, _ = ctx.Deadline()
	f.calls++
	return f.window, f.err
}

func TestSearchTopicMessages_MatchesKeysAndValuesAndReportsWindow(t *testing.T) {
	fetch := &fakeRecentTopicMessages{window: TopicMessageWindow{
		Messages: []TopicMessage{
			{Partition: 0, Offset: 1, Timestamp: time.UnixMilli(1), Key: []byte("order-1")},
			{Partition: 1, Offset: 2, Timestamp: time.UnixMilli(2), Value: []byte("order-created")},
			{Partition: 2, Offset: 3, Timestamp: time.UnixMilli(3), Value: []byte("ORDER")},
			{Partition: 2, Offset: 4, Timestamp: time.UnixMilli(4)},
		},
		Truncated: true,
	}}
	start := time.Now()
	got, err := NewSearchTopicMessages(fetch, 2*time.Second).Execute(context.Background(), "orders", "order")
	if err != nil {
		t.Fatal(err)
	}
	want := TopicMessageSearchResult{
		Messages:  []TopicMessage{fetch.window.Messages[1], fetch.window.Messages[0]},
		Scanned:   4,
		Truncated: true,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("search result mismatch (-want +got):\n%s", diff)
	}
	if fetch.topic != "orders" || fetch.deadline.Before(start.Add(2*time.Second)) {
		t.Errorf("fetch received topic %q and deadline %v", fetch.topic, fetch.deadline)
	}
}

func TestSearchTopicMessages_EmptyResultIsAnArray(t *testing.T) {
	fetch := &fakeRecentTopicMessages{}
	got, err := NewSearchTopicMessages(fetch, time.Second).Execute(context.Background(), "orders", "not found")
	if err != nil {
		t.Fatal(err)
	}
	if got.Messages == nil || len(got.Messages) != 0 || got.Scanned != 0 || got.Truncated {
		t.Errorf("result = %#v, want empty non-nil messages and no exclusions", got)
	}
}

func TestSearchTopicMessages_RejectsInvalidQueryBeforeFetch(t *testing.T) {
	for _, query := range []string{"", string([]byte{0xff})} {
		fetch := &fakeRecentTopicMessages{}
		_, err := NewSearchTopicMessages(fetch, time.Second).Execute(context.Background(), "orders", query)
		if !errors.Is(err, ErrInvalidTopicMessageSearch) || fetch.calls != 0 {
			t.Errorf("query %q: error = %v, fetch calls = %d", query, err, fetch.calls)
		}
	}
}

func TestSearchTopicMessages_PropagatesErrorsAndRejectsOversizedWindow(t *testing.T) {
	failure := errors.New("Kafka unavailable")
	fetch := &fakeRecentTopicMessages{err: failure}
	_, err := NewSearchTopicMessages(fetch, time.Second).Execute(context.Background(), "orders", "x")
	if !errors.Is(err, failure) {
		t.Errorf("error = %v, want %v", err, failure)
	}

	fetch.err = nil
	fetch.window.Messages = make([]TopicMessage, TopicMessageSearchLimit+1)
	if _, err := NewSearchTopicMessages(fetch, time.Second).Execute(context.Background(), "orders", "x"); err == nil {
		t.Error("expected an error for an oversized search window")
	}
}
