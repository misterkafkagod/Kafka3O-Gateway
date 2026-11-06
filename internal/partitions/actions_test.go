package partitions

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type placementFake struct {
	placement Placement
	err       error
	limited   bool
}

func (f *placementFake) Execute(ctx context.Context, _ string) (Placement, error) {
	_, f.limited = ctx.Deadline()
	return f.placement, f.err
}

type updateFake struct {
	calls       []bool
	count       int32
	validateErr error
	applyErr    error
}

func (f *updateFake) Execute(_ context.Context, _ string, count int32, validateOnly bool) error {
	f.calls = append(f.calls, validateOnly)
	f.count = count
	if validateOnly {
		return f.validateErr
	}
	return f.applyErr
}

type listFake struct {
	ids    []int32
	active []Reassignment
	err    error
}

func (f *listFake) Execute(_ context.Context, _ string, ids []int32) ([]Reassignment, error) {
	f.ids = ids
	return f.active, f.err
}

type alterFake struct {
	calls     int
	partition int32
	replicas  []int32
	err       error
}

func (f *alterFake) Execute(_ context.Context, _ string, partition int32, replicas []int32) error {
	f.calls++
	f.partition = partition
	f.replicas = replicas
	return f.err
}

func testPlacement() *placementFake {
	return &placementFake{placement: Placement{
		Brokers: []int32{1, 2, 3},
		Partitions: map[int32][]int32{
			0: {1, 2},
			1: {2, 3},
		},
	}}
}

func TestIncreasePartitionCountValidatesBeforeOneApply(t *testing.T) {
	describe, update := testPlacement(), &updateFake{}
	err := NewIncreasePartitionCount(describe, update, time.Second).Execute(context.Background(), "orders", 4)
	if err != nil || !describe.limited || update.count != 4 || !reflect.DeepEqual(update.calls, []bool{true, false}) {
		t.Fatalf("error %v, describe %#v, update %#v", err, describe, update)
	}
}

func TestIncreasePartitionCountStopsBeforeUnsafeWrites(t *testing.T) {
	for _, test := range []struct {
		name        string
		topic       string
		count       int32
		describeErr error
		validateErr error
		want        error
		calls       []bool
	}{
		{"invalid topic", "..", 3, nil, nil, ErrInvalidChange, nil},
		{"nonpositive", "orders", 0, nil, nil, ErrInvalidChange, nil},
		{"unchanged", "orders", 2, nil, nil, ErrInvalidChange, nil},
		{"decrease", "orders", 1, nil, nil, ErrInvalidChange, nil},
		{"metadata failure", "orders", 3, ErrNotFound, nil, ErrNotFound, nil},
		{"Kafka validation failure", "orders", 3, nil, ErrRejected, ErrRejected, []bool{true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			describe, update := testPlacement(), &updateFake{validateErr: test.validateErr}
			describe.err = test.describeErr
			err := NewIncreasePartitionCount(describe, update, time.Second).Execute(context.Background(), test.topic, test.count)
			if !errors.Is(err, test.want) || !reflect.DeepEqual(update.calls, test.calls) {
				t.Fatalf("error %v, calls %v; want %v, %v", err, update.calls, test.want, test.calls)
			}
		})
	}
}

func TestIncreasePartitionCountReportsUnknownApply(t *testing.T) {
	update := &updateFake{applyErr: context.DeadlineExceeded}
	err := NewIncreasePartitionCount(testPlacement(), update, time.Second).Execute(context.Background(), "orders", 3)
	if !errors.Is(err, ErrWriteUnknown) || !reflect.DeepEqual(update.calls, []bool{true, false}) {
		t.Fatalf("error %v, calls %v", err, update.calls)
	}
}

func TestReassignPartitionRejectsUnsafeAssignmentsBeforeWrite(t *testing.T) {
	for _, test := range []struct {
		name      string
		topic     string
		partition int32
		replicas  []int32
		active    []Reassignment
		listErr   error
		want      error
	}{
		{"invalid topic", "..", 0, []int32{2}, nil, nil, ErrInvalidChange},
		{"negative partition", "orders", -1, []int32{2}, nil, nil, ErrInvalidChange},
		{"missing partition", "orders", 5, []int32{2}, nil, nil, ErrNotFound},
		{"nil cannot cancel", "orders", 0, nil, nil, nil, ErrInvalidChange},
		{"empty cannot cancel", "orders", 0, []int32{}, nil, nil, ErrInvalidChange},
		{"unknown broker", "orders", 0, []int32{4}, nil, nil, ErrInvalidChange},
		{"negative broker", "orders", 0, []int32{-1}, nil, nil, ErrInvalidChange},
		{"duplicate broker", "orders", 0, []int32{1, 1}, nil, nil, ErrInvalidChange},
		{"same assignment", "orders", 0, []int32{1, 2}, nil, nil, ErrInvalidChange},
		{"already active", "orders", 0, []int32{2, 3}, []Reassignment{{Partition: 0}}, nil, ErrInProgress},
		{"cannot check status", "orders", 0, []int32{2, 3}, nil, ErrDenied, ErrDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			list, alter := &listFake{active: test.active, err: test.listErr}, &alterFake{}
			err := NewReassignPartition(testPlacement(), list, alter, time.Second).Execute(context.Background(), test.topic, test.partition, test.replicas)
			if !errors.Is(err, test.want) || alter.calls != 0 {
				t.Fatalf("error %v, writes %d; want %v and no writes", err, alter.calls, test.want)
			}
		})
	}
}

func TestReassignPartitionSubmitsOnceAndReportsUnknown(t *testing.T) {
	for _, applyErr := range []error{nil, context.DeadlineExceeded} {
		list, alter := &listFake{}, &alterFake{err: applyErr}
		err := NewReassignPartition(testPlacement(), list, alter, time.Second).Execute(context.Background(), "orders", 0, []int32{2, 3})
		if (applyErr == nil && err != nil) || (applyErr != nil && !errors.Is(err, ErrWriteUnknown)) ||
			alter.calls != 1 || alter.partition != 0 || !reflect.DeepEqual(alter.replicas, []int32{2, 3}) ||
			!reflect.DeepEqual(list.ids, []int32{0}) {
			t.Fatalf("apply error %v, got %v; list %#v, alter %#v", applyErr, err, list, alter)
		}
	}
}

func TestListReassignmentsRequestsAllTopicPartitions(t *testing.T) {
	list := &listFake{active: []Reassignment{{Partition: 1}}}
	got, err := NewListReassignments(testPlacement(), list, time.Second).Execute(context.Background(), "orders")
	if err != nil || !reflect.DeepEqual(list.ids, []int32{0, 1}) || !reflect.DeepEqual(got, list.active) {
		t.Fatalf("error %v, ids %v, result %v", err, list.ids, got)
	}
}
