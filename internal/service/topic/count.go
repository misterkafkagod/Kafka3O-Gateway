package topic

import (
	"context"
	"sort"
)

// PartitionCount is one partition's message count within a window
// (FUNC-SPEC §8.7 T4).
type PartitionCount struct {
	ID         int32
	FromOffset int64
	ToOffset   int64
	Count      int64
}

// Count is CountInWindow's result (FUNC-SPEC §8.7 T4).
type Count struct {
	Topic      string
	FromMilli  int64
	ToMilli    int64
	Total      int64
	Partitions []PartitionCount
}

// CountInWindow returns the per-partition and total message count for name
// between fromMilli and toMilli, inclusive of fromMilli (FUNC-SPEC §8.7 T4).
// toOffset is clamped to the partition's end offset — a window reaching past
// the latest record never reports more than what exists (FUNC-SPEC §9.1
// rules, C9's "min(to, endSnapshot)" convention applied here to T4).
func (s *Service) CountInWindow(ctx context.Context, name string, fromMilli, toMilli int64) (Count, error) {
	fromOffsets, err := s.admin.ListOffsetsAfterMilli(ctx, name, fromMilli)
	if err != nil {
		return Count{}, err
	}
	toOffsets, err := s.admin.ListOffsetsAfterMilli(ctx, name, toMilli)
	if err != nil {
		return Count{}, err
	}
	endOffsets, err := s.admin.ListEndOffsets(ctx, name)
	if err != nil {
		return Count{}, err
	}

	ids := make([]int32, 0, len(fromOffsets))
	for id := range fromOffsets {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var total int64
	partitions := make([]PartitionCount, 0, len(ids))
	for _, id := range ids {
		from := fromOffsets[id]
		to := toOffsets[id]
		if end, ok := endOffsets[id]; ok && to > end {
			to = end
		}
		if to < from {
			to = from
		}
		count := to - from
		total += count
		partitions = append(partitions, PartitionCount{ID: id, FromOffset: from, ToOffset: to, Count: count})
	}

	return Count{Topic: name, FromMilli: fromMilli, ToMilli: toMilli, Total: total, Partitions: partitions}, nil
}
