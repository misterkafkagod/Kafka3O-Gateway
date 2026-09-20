// Package porttest is the shared contract suite for internal/kafka port
// implementations (TECH-SPEC L1): the same Run exercises internal/kafka/fake
// in the automated suite and internal/kafka/franz under the integration build
// tag at Level 2 (Task 16.2), so both adapters are held to identical error
// and context semantics (L2, L3). The package imports internal/kafka and the
// standard library only — never a concrete adapter — so it works unchanged
// against either one.
package porttest

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Run exercises every port surface port implements. Cases that need an
// optional test-only capability (seeding, forcing unavailability) run only
// when port also implements the matching capability interface — see admin.go
// — so the identical call works against a live cluster's adapter too.
func Run(t *testing.T, port kafka.Admin) {
	t.Helper()
	t.Run("Admin", func(t *testing.T) { RunAdmin(t, port) })
}
