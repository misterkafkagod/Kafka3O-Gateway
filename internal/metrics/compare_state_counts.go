package metrics

import "cmp"

// CompareStateCounts orders state counts by state name.
func CompareStateCounts(a, b StateCount) int {
	return cmp.Compare(a.State, b.State)
}
