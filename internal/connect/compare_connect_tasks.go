package connect

import "cmp"

// CompareConnectTasks orders tasks by ID, for slices.SortFunc.
func CompareConnectTasks(a, b Task) int {
	return cmp.Compare(a.ID, b.ID)
}
