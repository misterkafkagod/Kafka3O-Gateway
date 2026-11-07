package cognito

import (
	"slices"
)

func sameAudiences(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	if slices.Equal(first, second) {
		return true
	}
	first = slices.Clone(first)
	second = slices.Clone(second)
	slices.Sort(first)
	slices.Sort(second)
	return slices.Equal(first, second)
}
