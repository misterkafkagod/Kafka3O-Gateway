package topics

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
)

type fakeListTopicMetadata struct {
	topics   []TopicMetadata
	err      error
	deadline time.Time
	hasDL    bool
}

func (f *fakeListTopicMetadata) Execute(ctx context.Context) ([]TopicMetadata, error) {
	f.deadline, f.hasDL = ctx.Deadline()
	return f.topics, f.err
}

type fakeCountTopicMessages struct {
	counts   map[string]int64
	err      error
	names    []string
	deadline time.Time
	hasDL    bool
}

func (f *fakeCountTopicMessages) Execute(ctx context.Context, names []string) (map[string]int64, error) {
	f.names = names
	f.deadline, f.hasDL = ctx.Deadline()
	return f.counts, f.err
}

func TestListTopics_MergesCountsAndSortsByName(t *testing.T) {
	metadata := &fakeListTopicMetadata{topics: []TopicMetadata{
		{Name: "orders", Partitions: 6, ReplicationFactor: 3},
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 1, Internal: true},
		{Name: "audit", Partitions: 1, ReplicationFactor: 1},
	}}
	count := &fakeCountTopicMessages{counts: map[string]int64{"orders": 1000, "__consumer_offsets": 7}}

	got, err := NewListTopics(metadata, count, time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	want := []Topic{
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 1, Internal: true, ApproxMessages: 7},
		{Name: "audit", Partitions: 1, ReplicationFactor: 1, ApproxMessages: 0},
		{Name: "orders", Partitions: 6, ReplicationFactor: 3, ApproxMessages: 1000},
	}
	if diff := gocmp.Diff(want, got); diff != "" {
		t.Errorf("topics mismatch (-want +got):\n%s", diff)
	}
	gotNames := slices.Clone(count.names)
	slices.Sort(gotNames)
	if diff := gocmp.Diff([]string{"__consumer_offsets", "audit", "orders"}, gotNames); diff != "" {
		t.Errorf("counted names mismatch (-want +got):\n%s", diff)
	}
}

func TestListTopics_ReturnsEmptySliceForNoTopics(t *testing.T) {
	got, err := NewListTopics(&fakeListTopicMetadata{}, &fakeCountTopicMessages{}, time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %#v, want an empty non-nil slice", got)
	}
}

func TestListTopics_ReturnsMetadataError(t *testing.T) {
	boom := errors.New("boom")

	_, err := NewListTopics(&fakeListTopicMetadata{err: boom}, &fakeCountTopicMessages{}, time.Second).Execute(context.Background())

	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want %v", err, boom)
	}
}

func TestListTopics_ReturnsCountError(t *testing.T) {
	boom := errors.New("boom")
	metadata := &fakeListTopicMetadata{topics: []TopicMetadata{{Name: "orders"}}}

	_, err := NewListTopics(metadata, &fakeCountTopicMessages{err: boom}, time.Second).Execute(context.Background())

	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want %v", err, boom)
	}
}

func TestListTopics_PassesConfiguredDeadlineToBothPorts(t *testing.T) {
	metadata := &fakeListTopicMetadata{}
	count := &fakeCountTopicMessages{}
	start := time.Now()

	if _, err := NewListTopics(metadata, count, 2*time.Second).Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	end := time.Now()

	for name, dl := range map[string]struct {
		deadline time.Time
		has      bool
	}{"metadata": {metadata.deadline, metadata.hasDL}, "count": {count.deadline, count.hasDL}} {
		if !dl.has {
			t.Fatalf("%s port got no deadline", name)
		}
		if dl.deadline.Before(start.Add(2*time.Second)) || dl.deadline.After(end.Add(2*time.Second)) {
			t.Errorf("%s deadline %v not within [start+2s, end+2s]", name, dl.deadline)
		}
	}
}
