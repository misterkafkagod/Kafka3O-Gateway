package core_test

import (
	"regexp"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestPlanToken_OrderIndependent(t *testing.T) {
	t.Parallel()
	a := core.Token("T8", []string{"tmp-a", "tmp-b", "tmp-c"})
	b := core.Token("T8", []string{"tmp-c", "tmp-a", "tmp-b"})
	if a != b {
		t.Errorf("Token() = %q and %q for the same targets in different order, want equal", a, b)
	}
}

func TestPlanToken_DuplicatesCollapsed(t *testing.T) {
	t.Parallel()
	withDup := core.Token("T8", []string{"tmp-a", "tmp-a", "tmp-b"})
	without := core.Token("T8", []string{"tmp-a", "tmp-b"})
	if withDup != without {
		t.Errorf("Token() = %q with a duplicate target, %q without, want equal", withDup, without)
	}
}

func TestPlanToken_DistinctPerCommandID(t *testing.T) {
	t.Parallel()
	a := core.Token("T8", []string{"tmp-a"})
	b := core.Token("C9", []string{"tmp-a"})
	if a == b {
		t.Errorf("Token() = %q for both T8 and C9 with the same targets, want distinct", a)
	}
}

func TestPlanToken_Is64LowercaseHex(t *testing.T) {
	t.Parallel()
	hex64 := regexp.MustCompile(`^[0-9a-f]{64}$`)
	got := core.Token("T8", []string{"tmp-a", "tmp-b"})
	if !hex64.MatchString(got) {
		t.Errorf("Token() = %q, want 64 lower-case hex characters", got)
	}
}

func TestPlanToken_KnownVector(t *testing.T) {
	t.Parallel()
	// sha256("T8\na\nb") — TECH-SPEC C8's canonical form applied by hand.
	const want = "07385a1fc399b60dce5dd430886d3c522baf78b46ff40622150d80de62ead7d3"
	got := core.Token("T8", []string{"b", "a"})
	if got != want {
		t.Errorf("Token(\"T8\", [b a]) = %q, want %q", got, want)
	}
}
