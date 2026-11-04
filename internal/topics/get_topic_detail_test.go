package topics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

type fakeDescribeTopic struct {
	detail   TopicDetail
	err      error
	name     string
	deadline time.Time
	hasDL    bool
}

func (f *fakeDescribeTopic) Execute(ctx context.Context, name string) (TopicDetail, error) {
	f.name = name
	f.deadline, f.hasDL = ctx.Deadline()
	return f.detail, f.err
}

func TestGetTopicDetail_ReturnsDetail(t *testing.T) {
	want := TopicDetail{
		Name: "orders",
		Partitions: []TopicPartition{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1}, StartOffset: 3, EndOffset: 9},
		},
		Config: []TopicConfig{{Name: "cleanup.policy", Value: "compact"}},
	}
	describe := &fakeDescribeTopic{detail: want}

	got, err := NewGetTopicDetail(describe, time.Second).Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("detail mismatch (-want +got):\n%s", diff)
	}
	if describe.name != "orders" {
		t.Errorf("requested topic = %q, want orders", describe.name)
	}
}

func TestGetTopicDetail_PassesErrorsThrough(t *testing.T) {
	want := errors.New("topic lookup failed")
	describe := &fakeDescribeTopic{err: want}

	_, err := NewGetTopicDetail(describe, time.Second).Execute(context.Background(), "orders")
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}

func TestGetTopicDetail_PassesConfiguredDeadline(t *testing.T) {
	describe := &fakeDescribeTopic{}
	start := time.Now()

	_, err := NewGetTopicDetail(describe, 2*time.Second).Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	end := time.Now()

	if !describe.hasDL {
		t.Fatal("DescribeTopic received no deadline")
	}
	if describe.deadline.Before(start.Add(2*time.Second)) || describe.deadline.After(end.Add(2*time.Second)) {
		t.Errorf("deadline %v not within [start+2s, end+2s]", describe.deadline)
	}
}

func TestGetTopicDetail_PropagatesMissingTopic(t *testing.T) {
	describe := &fakeDescribeTopic{err: ErrTopicNotFound}

	_, err := NewGetTopicDetail(describe, time.Second).Execute(context.Background(), "missing")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("err = %v, want ErrTopicNotFound", err)
	}
}
