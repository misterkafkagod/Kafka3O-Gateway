package metrics

import "cmp"

// CompareLogDirs orders log directories by broker, then path, for slices.SortFunc.
func CompareLogDirs(a, b LogDir) int {
	return cmp.Or(cmp.Compare(a.Broker, b.Broker), cmp.Compare(a.Dir, b.Dir))
}
