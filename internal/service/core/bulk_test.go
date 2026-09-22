package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestBulk_OneInvalidItemNothingExecutesNoAttempt(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)
	executed := 0

	_, err := core.BulkRun(context.Background(), auditor, audit.Event{EventID: "e1"},
		func(core.BulkSummary) audit.Event { return audit.Event{EventID: "e1"} },
		[]int{1, -1, 2},
		func(item int) error {
			if item < 0 {
				return errors.New("must be positive")
			}
			return nil
		},
		func(context.Context, int) error {
			executed++
			return nil
		},
	)

	if !core.IsCode(err, core.BulkValidationFailed) {
		t.Fatalf("BulkRun() error = %v, want *core.PolicyError{Code: BulkValidationFailed}", err)
	}
	if executed != 0 {
		t.Errorf("execute ran %d times, want 0 (nothing executes on validation failure)", executed)
	}
	if events := rec.Events(); len(events) != 0 {
		t.Errorf("Events() = %+v, want none (no ATTEMPT on validation failure)", events)
	}
}

func TestBulk_AllValidAttemptThenExecuteThenResult(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)
	var executedOrder []int

	result, err := core.BulkRun(context.Background(), auditor, audit.Event{EventID: "e1", CommandID: "M5"},
		func(summary core.BulkSummary) audit.Event {
			return audit.Event{EventID: "e1", CommandID: "M5", Outcome: audit.OutcomeSucceeded}
		},
		[]int{10, 20, 30},
		func(int) error { return nil },
		func(_ context.Context, item int) error {
			executedOrder = append(executedOrder, item)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("BulkRun() error: %v", err)
	}
	if got, want := executedOrder, []int{10, 20, 30}; !equalInts(got, want) {
		t.Errorf("execute order = %v, want %v", got, want)
	}
	if result.Summary != (core.BulkSummary{Total: 3, Succeeded: 3, Failed: 0}) {
		t.Errorf("Summary = %+v, want {Total:3 Succeeded:3 Failed:0}", result.Summary)
	}

	events := rec.Events()
	if len(events) != 2 {
		t.Fatalf("Events() = %d, want 2 (ATTEMPT then RESULT)", len(events))
	}
	if events[0].Phase != audit.PhaseAttempt || events[1].Phase != audit.PhaseResult {
		t.Errorf("phases = [%s, %s], want [ATTEMPT, RESULT]", events[0].Phase, events[1].Phase)
	}
}

func TestBulk_ExecutionFailureIsMixedWithPerItemStatus(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)

	result, err := core.BulkRun(context.Background(), auditor, audit.Event{EventID: "e1"},
		func(core.BulkSummary) audit.Event { return audit.Event{EventID: "e1"} },
		[]int{1, 2, 3},
		func(int) error { return nil },
		func(_ context.Context, item int) error {
			if item == 2 {
				return errors.New("broker rejected item 2")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("BulkRun() error: %v", err)
	}
	if len(result.Items) != 3 {
		t.Fatalf("Items = %+v, want 3 entries", result.Items)
	}
	if result.Items[0].Outcome != audit.OutcomeSucceeded || result.Items[2].Outcome != audit.OutcomeSucceeded {
		t.Errorf("Items[0], Items[2] outcomes = %s, %s, want SUCCEEDED both", result.Items[0].Outcome, result.Items[2].Outcome)
	}
	if result.Items[1].Outcome != audit.OutcomeFailed || result.Items[1].Error == "" {
		t.Errorf("Items[1] = %+v, want Outcome FAILED with a non-empty Error", result.Items[1])
	}
}

func TestBulk_SummaryCounts(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	auditor := audit.NewAuditor(rec)

	result, err := core.BulkRun(context.Background(), auditor, audit.Event{EventID: "e1"},
		func(core.BulkSummary) audit.Event { return audit.Event{EventID: "e1"} },
		[]int{1, 2, 3, 4},
		func(int) error { return nil },
		func(_ context.Context, item int) error {
			if item%2 == 0 {
				return errors.New("even items fail")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("BulkRun() error: %v", err)
	}
	want := core.BulkSummary{Total: 4, Succeeded: 2, Failed: 2}
	if result.Summary != want {
		t.Errorf("Summary = %+v, want %+v", result.Summary, want)
	}
}

func TestBulk_AttemptSinkFailureIsAuditUnavailableNoExecute(t *testing.T) {
	t.Parallel()
	rec := audittest.New()
	rec.FailNext(audit.PhaseAttempt)
	auditor := audit.NewAuditor(rec)
	executed := 0

	_, err := core.BulkRun(context.Background(), auditor, audit.Event{EventID: "e1"},
		func(core.BulkSummary) audit.Event { return audit.Event{EventID: "e1"} },
		[]int{1, 2},
		func(int) error { return nil },
		func(context.Context, int) error {
			executed++
			return nil
		},
	)

	if !core.IsCode(err, core.AuditUnavailable) {
		t.Fatalf("BulkRun() error = %v, want *core.PolicyError{Code: AuditUnavailable}", err)
	}
	if executed != 0 {
		t.Errorf("execute ran %d times, want 0 (fail-closed on ATTEMPT sink failure)", executed)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
