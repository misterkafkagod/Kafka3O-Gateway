package kafka

import (
	"fmt"
	"slices"
)

func allocateRecentTopicOffsets(starts, ends map[int32]int64, limit int) (map[int32]int64, map[int32]int64, bool, error) {
	if limit < 0 || len(starts) != len(ends) {
		return nil, nil, false, fmt.Errorf("invalid search window bounds")
	}
	partitions := make([]int32, 0, len(ends))
	next := make(map[int32]int64, len(ends))
	for partition, end := range ends {
		start, ok := starts[partition]
		if !ok || start < 0 || end < start {
			return nil, nil, false, fmt.Errorf("invalid offsets for partition %d", partition)
		}
		partitions = append(partitions, partition)
		next[partition] = end
	}
	slices.Sort(partitions)
	remaining := limit
	for remaining > 0 {
		progress := false
		for _, partition := range partitions {
			if next[partition] > starts[partition] {
				next[partition]--
				remaining--
				progress = true
				if remaining == 0 {
					break
				}
			}
		}
		if !progress {
			break
		}
	}

	selectedStarts := make(map[int32]int64)
	selectedEnds := make(map[int32]int64)
	truncated := false
	for _, partition := range partitions {
		if next[partition] > starts[partition] {
			truncated = true
		}
		if next[partition] < ends[partition] {
			selectedStarts[partition] = next[partition]
			selectedEnds[partition] = ends[partition]
		}
	}
	return selectedStarts, selectedEnds, truncated, nil
}
