package app

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/config"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestPolicy_FromConfigSwitches(t *testing.T) {
	t.Parallel()

	got := mapPolicy(true, config.Policy{
		ReadOnlyMode:       true,
		DataPlaneLock:      true,
		DisabledOperations: []string{"T7", "T11"},
	})

	want := core.Policy{
		AuthEnabled:   true,
		ReadOnly:      true,
		DataPlaneLock: true,
		Disabled:      map[string]bool{"T7": true, "T11": true},
	}
	if got.AuthEnabled != want.AuthEnabled || got.ReadOnly != want.ReadOnly || got.DataPlaneLock != want.DataPlaneLock {
		t.Fatalf("mapPolicy() = %+v, want %+v", got, want)
	}
	if len(got.Disabled) != len(want.Disabled) {
		t.Fatalf("Disabled = %v, want %v", got.Disabled, want.Disabled)
	}
	for id := range want.Disabled {
		if !got.Disabled[id] {
			t.Errorf("Disabled[%q] = false, want true", id)
		}
	}
}

func TestPolicy_FromConfigSwitches_AllOff(t *testing.T) {
	t.Parallel()

	got := mapPolicy(false, config.Policy{})

	if got.AuthEnabled || got.ReadOnly || got.DataPlaneLock || len(got.Disabled) != 0 {
		t.Errorf("mapPolicy() = %+v, want every switch off and no disabled operations", got)
	}
}
