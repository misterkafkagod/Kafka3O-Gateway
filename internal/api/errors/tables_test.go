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
