package cluster

import "cmp"

// CompareBrokersByID orders brokers by ascending ID, for slices.SortFunc.
func CompareBrokersByID(a, b Broker) int {
	return cmp.Compare(a.ID, b.ID)
}
