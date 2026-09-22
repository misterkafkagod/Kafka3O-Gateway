package core_test

import (
	"strings"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// FuzzPlanTokenCanonical proves Token is invariant under reordering its
// targets (TECH-SPEC C8, Step 11 G1): split fuzzed input into a target list
// and compare the token computed in forward and reverse order.
func FuzzPlanTokenCanonical(f *testing.F) {
	for _, seed := range []string{
		"", "a", "a,b,c", "tmp-a,tmp-b,tmp-c", "a,a,b", "日本語,topic", "a\nb,c\td",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		targets := strings.Split(raw, ",")

		reversed := make([]string, len(targets))
		for i, target := range targets {
			reversed[len(targets)-1-i] = target
		}

		forward := core.Token("T8", targets)
		backward := core.Token("T8", reversed)
		if forward != backward {
			t.Fatalf("Token() = %q forward, %q reversed, for the same targets reordered, want equal", forward, backward)
		}
	})
}
