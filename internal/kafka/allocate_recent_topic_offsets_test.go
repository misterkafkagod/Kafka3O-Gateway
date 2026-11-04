package kafka

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestAllocateRecentTopicOffsets_FairBoundedWindows(t *testing.T) {
	tests := []struct {
		name       string
		starts     map[int32]int64
		ends       map[int32]int64
		limit      int
		wantStarts map[int32]int64
		wantEnds   map[int32]int64
		truncated  bool
	}{
		{name: "empty", starts: map[int32]int64{}, ends: map[int32]int64{}, limit: 500, wantStarts: map[int32]int64{}, wantEnds: map[int32]int64{}},
		{name: "all positions", starts: map[int32]int64{0: 100, 1: 5}, ends: map[int32]int64{0: 103, 1: 5}, limit: 500, wantStarts: map[int32]int64{0: 100}, wantEnds: map[int32]int64{0: 103}},
		{name: "uneven windows", starts: map[int32]int64{0: 0, 1: 0}, ends: map[int32]int64{0: 400, 1: 200}, limit: 500, wantStarts: map[int32]int64{0: 100, 1: 0}, wantEnds: map[int32]int64{0: 400, 1: 200}, truncated: true},
		{name: "nonzero sparse offsets", starts: map[int32]int64{4: 10000, 9: 2}, ends: map[int32]int64{4: 11000, 9: 4}, limit: 5, wantStarts: map[int32]int64{4: 10997, 9: 2}, wantEnds: map[int32]int64{4: 11000, 9: 4}, truncated: true},
		{name: "zero limit", starts: map[int32]int64{0: 0}, ends: map[int32]int64{0: 1}, limit: 0, wantStarts: map[int32]int64{}, wantEnds: map[int32]int64{}, truncated: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			starts, ends, truncated, err := allocateRecentTopicOffsets(tt.starts, tt.ends, tt.limit)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantStarts, starts); diff != "" {
				t.Errorf("starts mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantEnds, ends); diff != "" {
				t.Errorf("ends mismatch (-want +got):\n%s", diff)
			}
			if truncated != tt.truncated {
				t.Errorf("truncated = %v, want %v", truncated, tt.truncated)
			}
			var total int64
			for partition, start := range starts {
				total += ends[partition] - start
			}
			if total > int64(tt.limit) {
				t.Errorf("selected %d offsets, limit %d", total, tt.limit)
			}
		})
	}
}

func TestAllocateRecentTopicOffsets_MoreThan500Partitions(t *testing.T) {
	starts := make(map[int32]int64)
	ends := make(map[int32]int64)
	for partition := int32(0); partition < 600; partition++ {
		starts[partition] = 0
		ends[partition] = 2
	}
	selected, selectedEnds, truncated, err := allocateRecentTopicOffsets(starts, ends, 500)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated || len(selected) != 500 {
		t.Fatalf("selected %d partitions, truncated = %v; want 500 and true", len(selected), truncated)
	}
	for partition := int32(0); partition < 500; partition++ {
		if selected[partition] != 1 || selectedEnds[partition] != 2 {
			t.Errorf("partition %d has range [%d, %d), want [1, 2)", partition, selected[partition], selectedEnds[partition])
		}
	}
}

func TestAllocateRecentTopicOffsets_RejectsInvalidBounds(t *testing.T) {
	tests := []struct {
		starts map[int32]int64
		ends   map[int32]int64
		limit  int
	}{
		{map[int32]int64{0: 0}, map[int32]int64{1: 1}, 500},
		{map[int32]int64{0: 2}, map[int32]int64{0: 1}, 500},
		{map[int32]int64{0: -1}, map[int32]int64{0: 1}, 500},
		{map[int32]int64{}, map[int32]int64{}, -1},
	}
	for _, tt := range tests {
		if _, _, _, err := allocateRecentTopicOffsets(tt.starts, tt.ends, tt.limit); err == nil {
			t.Errorf("expected error for starts %v, ends %v, limit %d", tt.starts, tt.ends, tt.limit)
		}
	}
}
