package topics

import "strings"

// CompareTopicsByName orders topics by ascending name, for slices.SortFunc.
func CompareTopicsByName(a, b Topic) int {
	return strings.Compare(a.Name, b.Name)
}
