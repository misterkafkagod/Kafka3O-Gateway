package groups

import (
	"slices"
	"testing"
)

func TestComparePartitionOffsetResets(t *testing.T) {
	rows := []PartitionOffsetReset{{Partition: 2}, {Partition: 0}, {Partition: 1}}
	slices.SortFunc(rows, ComparePartitionOffsetResets)
	if rows[0].Partition != 0 || rows[1].Partition != 1 || rows[2].Partition != 2 {
		t.Fatalf("rows = %+v", rows)
	}
}
