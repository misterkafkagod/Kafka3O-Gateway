package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

type testPlan struct{ target string }

func (p testPlan) ConfirmTarget() string { return p.target }

func testDestructiveSetup() (core.Runner, *audit.Auditor, *audittest.RecordingSink) {
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)
	runner := core.Runner{Check: func(core.Caller, command.Descriptor, core.Policy) error { return nil }}
	return runner, auditor, rec
}

func testDescriptor() command.Descriptor {
	return command.Descriptor{ID: "T9", Name: "Alter topic configuration", Access: command.W, Destructive: true}
}

func TestDestructive_MissingConfirmIsConfirmationMismatch(t *testing.T) {
	t.Parallel()
	runner, auditor, rec := testDestructiveSetup()

	_, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "", false,
		func() (testPlan, error) { return testPlan{target: "t-new"}, nil },
		func(testPlan) (string, error) { return "applied", nil },
	)

	if !core.IsCode(err, core.ConfirmationMismatch) {
		t.Fatalf("Destructive() error = %v, want *core.PolicyError{Code: ConfirmationMismatch}", err)
	}
	if events := rec.Events(); len(events) != 0 {
		t.Errorf("Events() = %+v, want none (no audit on a confirm mismatch)", events)
	}
}

func TestDestructive_WrongConfirmIsConfirmationMismatch(t *testing.T) {
	t.Parallel()
	runner, auditor, rec := testDestructiveSetup()

	_, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "wrong-name", false,
		func() (testPlan, error) { return testPlan{target: "t-new"}, nil },
		func(testPlan) (string, error) { return "applied", nil },
	)

	var pe *core.PolicyError
	if !errors.As(err, &pe) || pe.Code != core.ConfirmationMismatch {
		t.Fatalf("Destructive() error = %v, want *core.PolicyError{Code: ConfirmationMismatch}", err)
	}
	plan, ok := pe.Details["plan"].(testPlan)
	if !ok || plan.target != "t-new" {
		t.Errorf("Details[plan] = %v, want the fresh plan carrying t-new", pe.Details["plan"])
	}
	if events := rec.Events(); len(events) != 0 {
		t.Errorf("Events() = %+v, want none", events)
	}
}

func TestDestructive_DryRunReturnsPlanWithZeroMutatingCallsAndSingleResultAudit(t *testing.T) {
	t.Parallel()
	runner, auditor, rec := testDestructiveSetup()
	applyCalled := false

	result, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "t-new", true,
		func() (testPlan, error) { return testPlan{target: "t-new"}, nil },
		func(testPlan) (string, error) { applyCalled = true; return "applied", nil },
	)

	if err != nil {
		t.Fatalf("Destructive() error: %v", err)
	}
	if !result.DryRun || result.Plan.target != "t-new" {
		t.Fatalf("Destructive() = %+v, want DryRun true, Plan.target t-new", result)
	}
	if applyCalled {
		t.Error("apply was called during a dry-run, want zero mutating calls")
	}

	events := rec.Events()
	if len(events) != 1 || events[0].Phase != audit.PhaseResult {
		t.Fatalf("Events() = %+v, want exactly one RESULT (no ATTEMPT on a dry-run)", events)
	}
	if !events[0].DryRun || events[0].Outcome != audit.OutcomeSucceeded || events[0].Severity != audit.SeverityInfo {
		t.Errorf("event = %+v, want DryRun true, Outcome SUCCEEDED, Severity INFO", events[0])
	}
}

func TestDestructive_AttemptThenApplyThenResultSucceeded(t *testing.T) {
	t.Parallel()
	runner, auditor, rec := testDestructiveSetup()
	var applyCalledWith testPlan

	result, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "t-new", false,
		func() (testPlan, error) { return testPlan{target: "t-new"}, nil },
		func(p testPlan) (string, error) { applyCalledWith = p; return "applied", nil },
	)

	if err != nil {
		t.Fatalf("Destructive() error: %v", err)
	}
	if result.DryRun || result.Value != "applied" {
		t.Fatalf("Destructive() = %+v, want DryRun false, Value applied", result)
	}
	if applyCalledWith.target != "t-new" {
		t.Errorf("apply was called with %+v, want the resolved plan", applyCalledWith)
	}

	events := rec.Events()
	if len(events) != 2 || events[0].Phase != audit.PhaseAttempt || events[1].Phase != audit.PhaseResult {
		t.Fatalf("Events() = %+v, want [ATTEMPT, RESULT]", events)
	}
	if events[0].EventID != events[1].EventID {
		t.Errorf("ATTEMPT/RESULT EventID mismatch: %q vs %q", events[0].EventID, events[1].EventID)
	}
	if events[1].Outcome != audit.OutcomeSucceeded {
		t.Errorf("RESULT.Outcome = %s, want SUCCEEDED", events[1].Outcome)
	}
}

func TestDestructive_ApplyErrorEmitsResultFailed(t *testing.T) {
	t.Parallel()
	runner, auditor, rec := testDestructiveSetup()
	applyErr := errors.New("broker rejected the alter")

	_, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "t-new", false,
		func() (testPlan, error) { return testPlan{target: "t-new"}, nil },
		func(testPlan) (string, error) { return "", applyErr },
	)

	if !errors.Is(err, applyErr) {
		t.Fatalf("Destructive() error = %v, want %v", err, applyErr)
	}

	events := rec.Events()
	if len(events) != 2 || events[1].Phase != audit.PhaseResult {
		t.Fatalf("Events() = %+v, want [ATTEMPT, RESULT]", events)
	}
	if events[1].Outcome != audit.OutcomeFailed {
		t.Errorf("RESULT.Outcome = %s, want FAILED", events[1].Outcome)
	}
	if events[1].Error == nil || events[1].Error.Code == "" {
		t.Errorf("RESULT.Error = %v, want a non-empty code", events[1].Error)
	}
}

func TestDestructive_DisabledOperationIs403BeforePlan(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)
	runner := core.Runner{
		Policy: core.Policy{Disabled: map[string]bool{"T9": true}},
		Check: func(caller core.Caller, d command.Descriptor, p core.Policy) error {
			if d.Access == command.W && p.Disabled[d.ID] {
				return &core.PolicyError{Code: core.OperationDisabled}
			}
			return nil
		},
	}
	planCalled := false

	_, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "t-new", false,
		func() (testPlan, error) { planCalled = true; return testPlan{target: "t-new"}, nil },
		func(testPlan) (string, error) { return "applied", nil },
	)

	if !core.IsCode(err, core.OperationDisabled) {
		t.Fatalf("Destructive() error = %v, want *core.PolicyError{Code: OperationDisabled}", err)
	}
	if planCalled {
		t.Error("plan was called despite the gate rejecting the request")
	}

	events := rec.Events()
	if len(events) != 1 || events[0].Outcome != audit.OutcomeRejected {
		t.Fatalf("Events() = %+v, want one REJECTED RESULT (from CheckAudited)", events)
	}
}

func TestDestructive_AttemptSinkFailureAbortsBeforeApply(t *testing.T) {
	t.Parallel()
	runner, auditor, rec := testDestructiveSetup()
	rec.FailNext(audit.PhaseAttempt)
	applyCalled := false

	_, err := core.Destructive(context.Background(), runner, auditor, core.Caller{Tier: core.TierOperator},
		testDescriptor(), audit.Event{EventID: "e1", CommandID: "T9"}, "t-new", false,
		func() (testPlan, error) { return testPlan{target: "t-new"}, nil },
		func(testPlan) (string, error) { applyCalled = true; return "applied", nil },
	)

	if !core.IsCode(err, core.AuditUnavailable) {
		t.Fatalf("Destructive() error = %v, want *core.PolicyError{Code: AuditUnavailable}", err)
	}
	if applyCalled {
		t.Error("apply was called despite the ATTEMPT sink failing (V2 fail-closed)")
	}
}
