package groups

import "cmp"

// ComparePartitionOffsetResets orders reset rows by ascending partition, for slices.SortFunc.
func ComparePartitionOffsetResets(a, b PartitionOffsetReset) int {
	return cmp.Compare(a.Partition, b.Partition)
}
