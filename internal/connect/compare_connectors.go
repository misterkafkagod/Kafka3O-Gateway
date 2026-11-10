package connect

import "cmp"

// CompareConnectors orders connectors by name, for slices.SortFunc.
func CompareConnectors(a, b Connector) int {
	return cmp.Compare(a.Name, b.Name)
}
