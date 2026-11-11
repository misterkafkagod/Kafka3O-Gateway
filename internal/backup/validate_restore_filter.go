package backup

import (
	"fmt"
	"slices"
)

// ValidateRestoreFilter refuses a time range whose start is not before its end and an empty or negative partition
// list, and returns the window and the sorted, distinct partitions (nil = all).
func ValidateRestoreFilter(filter RestoreFilter) (RecordWindow, []int, error) {
	window := AllRecords
	if filter.From != nil {
		window.From = filter.From.UnixMilli()
	}
	if filter.To != nil {
		window.To = filter.To.UnixMilli()
	}
	if window.From >= window.To {
		return RecordWindow{}, nil, fmt.Errorf("%w: the start of the time range must be before its end", ErrInvalidRestoreFilter)
	}
	if filter.Partitions == nil {
		return window, nil, nil
	}
	if len(filter.Partitions) == 0 {
		return RecordWindow{}, nil, fmt.Errorf("%w: choose at least one partition", ErrInvalidRestoreFilter)
	}
	partitions := slices.Clone(filter.Partitions)
	slices.Sort(partitions)
	if partitions[0] < 0 {
		return RecordWindow{}, nil, fmt.Errorf("%w: partitions cannot be negative", ErrInvalidRestoreFilter)
	}
	return window, slices.Compact(partitions), nil
}
