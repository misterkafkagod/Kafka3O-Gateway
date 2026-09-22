package core_test

import (
	"fmt"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// BenchmarkPlanToken measures Token's cost at 1 and 1 000 targets (TECH-SPEC
// §4.7, Step 11 G1).
func BenchmarkPlanToken(b *testing.B) {
	for _, n := range []int{1, 1000} {
		targets := make([]string, n)
		for i := range targets {
			targets[i] = fmt.Sprintf("tmp-%d", i)
		}

		name := fmt.Sprintf("%dtarget", n)
		if n != 1 {
			name += "s"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				core.Token("T8", targets)
			}
		})
	}
}
