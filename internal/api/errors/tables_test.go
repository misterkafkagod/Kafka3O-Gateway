package errors

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestErrorTables_EveryKindMapped(t *testing.T) {
	t.Parallel()
	for _, k := range kafka.Kinds() {
		m, ok := kindMapping(k)
		if !ok {
			t.Errorf("kafka.Kind %s has no row in kindTable", k)
			continue
		}
		if m.Status < 400 || m.Status > 599 || m.Code == "" {
			t.Errorf("kafka.Kind %s maps to invalid row %+v", k, m)
		}
	}
}

func TestErrorTables_EveryCodeMapped(t *testing.T) {
	t.Parallel()
	for _, c := range core.Codes() {
		m, ok := codeMapping(c)
		if !ok {
			t.Errorf("core.Code %s has no row in codeTable", c)
			continue
		}
		if m.Status < 400 || m.Status > 599 || m.Code == "" {
			t.Errorf("core.Code %s maps to invalid row %+v", c, m)
		}
	}
}

// TestErrorTables_ReassignmentInProgressIs409 names the exact row C9's
// reassign conflicts with an already-in-progress reassignment map to
// (FUNC-SPEC §8.4), beyond the two exhaustiveness tests above.
func TestErrorTables_ReassignmentInProgressIs409(t *testing.T) {
	t.Parallel()
	m, ok := kindMapping(kafka.KindReassignmentInProgress)
	if !ok || m.Status != 409 || m.Code != "REASSIGNMENT_IN_PROGRESS" {
		t.Fatalf("kindMapping(KindReassignmentInProgress) = %+v, %v, want {409, REASSIGNMENT_IN_PROGRESS}", m, ok)
	}
}
