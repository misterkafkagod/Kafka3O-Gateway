package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/topics"
)

func TestTopicMessageReadState_EmptyPolledPartitionDoesNotFinish(t *testing.T) {
	state, err := newTopicMessageReadState("orders", map[int32]int64{0: 0}, map[int32]int64{0: 3}, 50)
	if err != nil {
		t.Fatal(err)
	}
	err = state.accept(kgo.FetchTopicPartition{
		Topic: "orders",
		FetchPartition: kgo.FetchPartition{
			Partition: 0, HighWatermark: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.done[0] || state.next[0] != 0 || len(state.messages) != 0 {
		t.Fatalf("empty polled partition changed read state: %#v", state)
	}
}

func TestTopicMessageReadState_FinishesSparseSnapshot(t *testing.T) {
	state, err := newTopicMessageReadState("orders", map[int32]int64{0: 0, 1: 0}, map[int32]int64{0: 6, 1: 2}, 50)
	if err != nil {
		t.Fatal(err)
	}
	err = state.accept(kgo.FetchTopicPartition{
		Topic: "orders",
		FetchPartition: kgo.FetchPartition{
			Partition: 0, HighWatermark: 6,
			Records: []*kgo.Record{{Partition: 0, Offset: 0, Value: []byte("first")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var positions [][2]int64
	fetch := func(_ context.Context, partition int32, offset int64) (fetchedTopicPartition, error) {
		positions = append(positions, [2]int64{int64(partition), offset})
		switch {
		case partition == 0 && offset == 1:
			return fetchedTopicPartition{
				partition: kgo.FetchPartition{Partition: 0, HighWatermark: 6}, next: 4, hasBatch: true,
			}, nil
		case partition == 0 && offset == 4:
			return fetchedTopicPartition{
				partition: kgo.FetchPartition{
					Partition: 0, HighWatermark: 6,
					Records: []*kgo.Record{
						{Partition: 0, Offset: 4, Value: []byte("later")},
						{Partition: 0, Offset: 5, Attrs: kgo.NewRecordAttrs(kgo.RecordAttrsOpts{Control: true})},
					},
				},
				next: 6, hasBatch: true,
			}, nil
		case partition == 1 && offset == 0:
			return fetchedTopicPartition{
				partition: kgo.FetchPartition{
					Partition: 1, HighWatermark: 2,
					Records: []*kgo.Record{{Partition: 1, Offset: 1, Value: []byte("other")}},
				},
				next: 2, hasBatch: true,
			}, nil
		default:
			t.Fatalf("unexpected fetch at partition %d offset %d", partition, offset)
			return fetchedTopicPartition{}, nil
		}
	}
	if err := state.finish(context.Background(), fetch); err != nil {
		t.Fatal(err)
	}
	if want := [][2]int64{{0, 1}, {0, 4}, {1, 0}}; !reflect.DeepEqual(positions, want) {
		t.Fatalf("fetch positions = %v, want %v", positions, want)
	}
	want := []topics.TopicMessage{
		{Partition: 0, Offset: 0, OffsetExact: "0", Value: []byte("first"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 0, Offset: 4, OffsetExact: "4", Value: []byte("later"), Headers: []topics.TopicMessageHeader{}},
		{Partition: 1, Offset: 1, OffsetExact: "1", Value: []byte("other"), Headers: []topics.TopicMessageHeader{}},
	}
	if diff := cmp.Diff(want, state.messages); diff != "" {
		t.Fatalf("sparse messages mismatch (-want +got):\n%s", diff)
	}
}

func TestTopicMessageReadState_FinishRequiresProofOfCompletion(t *testing.T) {
	offline := errors.New("broker unavailable")
	tests := []struct {
		name       string
		result     fetchedTopicPartition
		fetchErr   error
		wantErr    string
		wantCause  error
		wantFinish bool
	}{
		{
			name: "empty single-partition response at snapshot end",
			result: fetchedTopicPartition{
				partition: kgo.FetchPartition{Partition: 0, HighWatermark: 3},
			},
			wantFinish: true,
		},
		{
			name: "watermark below snapshot",
			result: fetchedTopicPartition{
				partition: kgo.FetchPartition{Partition: 0, HighWatermark: 2},
			},
			wantErr: "high watermark",
		},
		{
			name: "unparseable batch",
			result: fetchedTopicPartition{
				partition: kgo.FetchPartition{Partition: 0, HighWatermark: 3}, next: 0, hasBatch: true,
			},
			wantErr: "no offset progress",
		},
		{name: "broker error", fetchErr: offline, wantCause: offline},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, err := newTopicMessageReadState("orders", map[int32]int64{0: 0}, map[int32]int64{0: 3}, 50)
			if err != nil {
				t.Fatal(err)
			}
			err = state.finish(context.Background(), func(context.Context, int32, int64) (fetchedTopicPartition, error) {
				return tt.result, tt.fetchErr
			})
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, tt.wantCause)
			}
			if tt.wantFinish && (err != nil || !state.done[0] || state.messages == nil || len(state.messages) != 0) {
				t.Fatalf("empty snapshot state = %#v, error = %v", state, err)
			}
			if !tt.wantFinish && state.done[0] {
				t.Fatal("an incomplete snapshot was marked done")
			}
		})
	}
}

func TestTopicMessageReadState_RejectsInvalidBounds(t *testing.T) {
	for _, tt := range []struct {
		offsets map[int32]int64
		ends    map[int32]int64
		limit   int
	}{
		{map[int32]int64{0: -1}, map[int32]int64{0: 1}, 50},
		{map[int32]int64{0: 2}, map[int32]int64{0: 1}, 50},
		{map[int32]int64{0: 0}, map[int32]int64{1: 1}, 50},
		{map[int32]int64{0: 0}, map[int32]int64{0: 1}, 0},
	} {
		if _, err := newTopicMessageReadState("orders", tt.offsets, tt.ends, tt.limit); err == nil {
			t.Fatalf("accepted invalid bounds: %#v", tt)
		}
	}
}
