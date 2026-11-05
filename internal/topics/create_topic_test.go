package topics

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeCreateKafkaTopic struct {
	calls   int
	request CreateTopicRequest
	hasDL   bool
	err     error
}

func (f *fakeCreateKafkaTopic) Execute(ctx context.Context, request CreateTopicRequest) error {
	f.calls++
	f.request = request
	_, f.hasDL = ctx.Deadline()
	return f.err
}

func TestCreateTopic_RejectsInvalidInput(t *testing.T) {
	for _, request := range []CreateTopicRequest{
		{Name: "", Partitions: 1, ReplicationFactor: 1},
		{Name: "..", Partitions: 1, ReplicationFactor: 1},
		{Name: "a/b", Partitions: 1, ReplicationFactor: 1},
		{Name: "orders", Partitions: 0, ReplicationFactor: 1},
		{Name: "orders", Partitions: 1, ReplicationFactor: 0},
	} {
		adapter := &fakeCreateKafkaTopic{}
		err := NewCreateTopic(adapter, time.Second).Execute(context.Background(), request)
		if !errors.Is(err, ErrInvalidTopic) || adapter.calls != 0 {
			t.Errorf("request = %#v, error = %v, calls = %d", request, err, adapter.calls)
		}
	}
}

func TestCreateTopic_ForwardsCountsDeadlineAndError(t *testing.T) {
	request := CreateTopicRequest{Name: "orders", Partitions: 3, ReplicationFactor: 2}
	want := errors.New("broker error")
	adapter := &fakeCreateKafkaTopic{err: want}
	err := NewCreateTopic(adapter, time.Second).Execute(context.Background(), request)
	if !errors.Is(err, want) || adapter.request != request || adapter.calls != 1 || !adapter.hasDL {
		t.Fatalf("error = %v, adapter = %#v", err, adapter)
	}
}
