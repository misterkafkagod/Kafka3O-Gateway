package gates

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// desc builds a test-only descriptor. gates.Check keys off these fields
// alone (TECH-SPEC O1), never off id, so any id string is valid here.
func desc(id string, access command.Access, dataPlane bool) command.Descriptor {
	return command.Descriptor{ID: id, Access: access, DataPlane: dataPlane}
}

func TestGatesCheck_Matrix(t *testing.T) {
	t.Parallel()

	operator := core.Caller{Tier: core.TierOperator}
	operatorBG := core.Caller{Tier: core.TierOperator, BreakGlassReason: "incident-1"}
	reader := core.Caller{Tier: core.TierReader}
	readerBG := core.Caller{Tier: core.TierReader, BreakGlassReason: "incident-1"}
	authOff := core.Caller{Tier: core.TierOperator} // node E: auth disabled resolves to operator upstream
	authOffBG := core.Caller{Tier: core.TierOperator, BreakGlassReason: "incident-1"}

	cases := []struct {
		name       string
		caller     core.Caller
		descriptor command.Descriptor
		policy     core.Policy
		wantCode   core.Code
		wantNil    bool
	}{
		// F=no (R command): G/H/I never evaluated, regardless of tier or switches.
		{"R, reader, no data-plane -> pass", reader, desc("T1", command.R, false), core.Policy{ReadOnly: true, DataPlaneLock: true}, 0, true},
		{"R, reader, data-plane, lock off -> pass (J=yes,K=no)", reader, desc("M1", command.R, true), core.Policy{}, 0, true},

		// J/K/K2 on an R command: F2/F3 never apply (I only guards W), but the lock still does.
		{"R, reader, data-plane, lock on, no header -> DataPlaneLocked (K2=no: tier)", reader, desc("M1", command.R, true), core.Policy{DataPlaneLock: true}, core.DataPlaneLocked, false},
		{"R, reader, data-plane, lock on, header but reader -> DataPlaneLocked (K2=no: tier)", readerBG, desc("M1", command.R, true), core.Policy{DataPlaneLock: true}, core.DataPlaneLocked, false},
		{"R, operator, data-plane, lock on, no header -> DataPlaneLocked (K2=no: header)", operator, desc("M1", command.R, true), core.Policy{DataPlaneLock: true}, core.DataPlaneLocked, false},
		{"R, operator, data-plane, lock on, header -> pass (K2=yes,K3)", operatorBG, desc("M1", command.R, true), core.Policy{DataPlaneLock: true}, 0, true},

		// F=yes (W command): G.
		{"W, reader -> TierForbidden (G=no)", reader, desc("T7", command.W, false), core.Policy{}, core.TierForbidden, false},
		{"W, operator -> pass (G=yes,H=no,I=yes,J=no)", operator, desc("T7", command.W, false), core.Policy{}, 0, true},

		// H.
		{"W, operator, readOnly -> ReadOnlyMode (H=yes)", operator, desc("T7", command.W, false), core.Policy{ReadOnly: true}, core.ReadOnlyMode, false},

		// I.
		{"W, operator, this op disabled -> OperationDisabled (I=no)", operator, desc("T7", command.W, false), core.Policy{Disabled: map[string]bool{"T7": true}}, core.OperationDisabled, false},
		{"W, operator, a different op disabled -> pass", operator, desc("T7", command.W, false), core.Policy{Disabled: map[string]bool{"T11": true}}, 0, true},

		// W + data-plane: F2/F3 are checked unconditionally, before J/K/K2.
		{"W data-plane, operator, lock off -> pass", operator, desc("M5", command.W, true), core.Policy{}, 0, true},
		{"W data-plane, operator, lock on, no header -> DataPlaneLocked", operator, desc("M5", command.W, true), core.Policy{DataPlaneLock: true}, core.DataPlaneLocked, false},
		{"W data-plane, operator, lock on, header -> pass (F6 bypassed)", operatorBG, desc("M5", command.W, true), core.Policy{DataPlaneLock: true}, 0, true},
		{"W data-plane, operator, readOnly AND lock, header -> ReadOnlyMode (break-glass never bypasses F2)", operatorBG, desc("M5", command.W, true), core.Policy{ReadOnly: true, DataPlaneLock: true}, core.ReadOnlyMode, false},
		{"W data-plane, operator, disabled AND lock, header -> OperationDisabled (break-glass never bypasses F3)", operatorBG, desc("M5", command.W, true), core.Policy{DataPlaneLock: true, Disabled: map[string]bool{"M5": true}}, core.OperationDisabled, false},
		{"W data-plane, reader, lock on, header -> TierForbidden (G blocks before J/K/K2 is ever reached)", readerBG, desc("M5", command.W, true), core.Policy{DataPlaneLock: true}, core.TierForbidden, false},

		// auth-off resolves to operator upstream (node E); Check behaves identically to a real operator key.
		{"auth-off, W -> pass, same as a real operator key", authOff, desc("T7", command.W, false), core.Policy{AuthEnabled: false}, 0, true},
		{"auth-off, R data-plane, lock on, no header -> DataPlaneLocked (auth-off is not break-glass)", authOff, desc("M1", command.R, true), core.Policy{AuthEnabled: false, DataPlaneLock: true}, core.DataPlaneLocked, false},
		{"auth-off, R data-plane, lock on, header -> pass", authOffBG, desc("M1", command.R, true), core.Policy{AuthEnabled: false, DataPlaneLock: true}, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Check(tc.caller, tc.descriptor, tc.policy)
			if tc.wantNil {
				if err != nil {
					t.Fatalf("Check() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Check() = nil, want Code %s", tc.wantCode)
			}
			if !core.IsCode(err, tc.wantCode) {
				t.Fatalf("Check() = %v, want Code %s", err, tc.wantCode)
			}
		})
	}
}

func TestGatesCheck_BreakGlassBypassesF6Only(t *testing.T) {
	t.Parallel()
	caller := core.Caller{Tier: core.TierOperator, BreakGlassReason: "incident-1"}
	descriptor := desc("M5", command.W, true)

	err := Check(caller, descriptor, core.Policy{ReadOnly: true, DataPlaneLock: true})
	if !core.IsCode(err, core.ReadOnlyMode) {
		t.Fatalf("Check() with read-only mode on = %v, want Code ReadOnlyMode (break-glass must not bypass F2)", err)
	}
}

func TestGatesCheck_ReaderBreakGlassIsDataPlaneLocked(t *testing.T) {
	t.Parallel()
	caller := core.Caller{Tier: core.TierReader, BreakGlassReason: "incident-1"}
	descriptor := desc("M1", command.R, true)

	err := Check(caller, descriptor, core.Policy{DataPlaneLock: true})
	if !core.IsCode(err, core.DataPlaneLocked) {
		t.Fatalf("Check() for a reader with the header = %v, want Code DataPlaneLocked", err)
	}
}

func BenchmarkGatesCheck(b *testing.B) {
	caller := core.Caller{Tier: core.TierOperator, BreakGlassReason: "incident-1"}
	descriptor := desc("M5", command.W, true)
	policy := core.Policy{DataPlaneLock: true, Disabled: map[string]bool{}}

	b.ReportAllocs()
	for b.Loop() {
		if err := Check(caller, descriptor, policy); err != nil {
			b.Fatal(err)
		}
	}
}
