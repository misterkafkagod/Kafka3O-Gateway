package metrics

import (
	"slices"
	"testing"
)

func TestCompareStateCounts(t *testing.T) {
	states := []StateCount{{State: "Stable"}, {State: "Empty"}, {State: "Dead"}}
	slices.SortFunc(states, CompareStateCounts)
	if states[0].State != "Dead" || states[2].State != "Stable" {
		t.Fatalf("order = %+v", states)
	}
}
