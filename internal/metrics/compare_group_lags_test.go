package metrics

import (
	"slices"
	"testing"
)

func TestCompareGroupLags(t *testing.T) {
	big, small, same := "9007199254740993", "9007199254740992", "5"
	groups := []GroupLag{{Group: "none"}, {Group: "b", Lag: &same}, {Group: "small", Lag: &small}, {Group: "a", Lag: &same}, {Group: "big", Lag: &big}, {Group: "alone"}}
	slices.SortFunc(groups, CompareGroupLags)
	names := []string{}
	for _, group := range groups {
		names = append(names, group.Group)
	}
	if want := []string{"big", "small", "a", "b", "alone", "none"}; !slices.Equal(names, want) {
		t.Fatalf("order = %v, want %v", names, want)
	}
}
