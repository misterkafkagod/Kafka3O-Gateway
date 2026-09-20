package core

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/command"
)

func TestRun_CallsGatesBeforeFn(t *testing.T) {
	t.Parallel()

	t.Run("fn does not run when Check rejects", func(t *testing.T) {
		t.Parallel()
		wantErr := &PolicyError{Code: TierForbidden}
		called := false
		r := Runner{Check: func(Caller, command.Descriptor, Policy) error { return wantErr }}

		_, err := run(r, context.Background(), Caller{}, command.Descriptor{}, func(context.Context) (int, error) {
			called = true
			return 42, nil
		})

		if !errors.Is(err, wantErr) {
			t.Errorf("run() error = %v, want %v", err, wantErr)
		}
		if called {
			t.Error("fn was called despite Check rejecting the request")
		}
	})

	t.Run("fn runs and its result is returned when Check passes", func(t *testing.T) {
		t.Parallel()
		var gotCaller Caller
		var gotDescriptor command.Descriptor
		var gotPolicy Policy
		r := Runner{
			Policy: Policy{ReadOnly: true},
			Check: func(c Caller, d command.Descriptor, p Policy) error {
				gotCaller, gotDescriptor, gotPolicy = c, d, p
				return nil
			},
		}
		caller := Caller{Tier: TierOperator, RequestID: "req-1"}
		descriptor := command.Descriptor{ID: "T1", Access: command.R}

		got, err := run(r, context.Background(), caller, descriptor, func(context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("run() error: %v", err)
		}
		if got != "ok" {
			t.Errorf("run() = %q, want %q", got, "ok")
		}
		if gotCaller != caller {
			t.Errorf("Check received caller = %+v, want %+v", gotCaller, caller)
		}
		if gotDescriptor != descriptor {
			t.Errorf("Check received descriptor = %+v, want %+v", gotDescriptor, descriptor)
		}
		if !gotPolicy.ReadOnly {
			t.Errorf("Check received policy.ReadOnly = false, want true (Runner.Policy)")
		}
	})
}
