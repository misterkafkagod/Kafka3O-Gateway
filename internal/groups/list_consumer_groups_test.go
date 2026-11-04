package groups

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type fakeConsumerGroups struct {
	snapshots []ConsumerGroupSnapshot
	err       error
	deadline  time.Time
}

type blockingConsumerGroups struct{}

func (blockingConsumerGroups) Execute(ctx context.Context) ([]ConsumerGroupSnapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeConsumerGroups) Execute(ctx context.Context) ([]ConsumerGroupSnapshot, error) {
	f.deadline, _ = ctx.Deadline()
	return f.snapshots, f.err
}

func TestListConsumerGroups_SortsAndReportsGroupErrors(t *testing.T) {
	fetch := &fakeConsumerGroups{snapshots: []ConsumerGroupSnapshot{
		{ID: "z", State: "Empty", Error: "not authorized"},
		{ID: "a", State: "Stable", Protocol: "classic", Members: []ConsumerGroupMember{{ID: "b"}, {ID: "a"}},
			Topics:     []string{"orders", "audit", "orders"},
			Partitions: []ConsumerPartitionSnapshot{{Topic: "orders", Partition: 2}, {Topic: "orders", Partition: 1}, {Topic: "audit", Partition: 0}}},
	}}
	start := time.Now()
	got, err := NewListConsumerGroups(fetch, time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != "a" || got[1].ID != "z" || got[1].Error != "not authorized" {
		t.Errorf("groups = %#v", got)
	}
	if diff := cmp.Diff([]string{"audit", "orders"}, got[0].Topics); diff != "" {
		t.Errorf("topics mismatch (-want +got):\n%s", diff)
	}
	if got[0].Members[0].ID != "a" || got[0].Partitions[0].Topic != "audit" || got[0].Partitions[1].Partition != 1 {
		t.Errorf("groups not sorted: %#v", got[0])
	}
	if got[1].Members == nil || got[1].Topics == nil || got[1].Partitions == nil {
		t.Error("empty group collections must be non-nil")
	}
	if fetch.deadline.Before(start.Add(time.Second)) {
		t.Errorf("fetch deadline %v is too early", fetch.deadline)
	}
}

func TestListConsumerGroups_EmptyAndError(t *testing.T) {
	fetch := &fakeConsumerGroups{}
	got, err := NewListConsumerGroups(fetch, time.Second).Execute(context.Background())
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("empty groups = %#v, error %v", got, err)
	}

	failure := errors.New("offline")
	fetch.err = failure
	if _, err := NewListConsumerGroups(fetch, time.Second).Execute(context.Background()); !errors.Is(err, failure) {
		t.Errorf("error = %v, want %v", err, failure)
	}
}

func TestListConsumerGroups_PropagatesTimeout(t *testing.T) {
	_, err := NewListConsumerGroups(blockingConsumerGroups{}, 10*time.Millisecond).Execute(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want deadline exceeded", err)
	}
}
