// Package core holds the transport-neutral pieces every service uses
// (TECH-SPEC §2.0 P1). This file is the generic FUNC-SPEC §9.4 bulk
// primitive: validate every item first (nothing executes on any failure,
// no ATTEMPT audit), then execute the valid items independently, each
// reporting its own outcome.
package core

import (
	"context"
	"fmt"

	"github.com/misterkafkagod/kafka3o/internal/audit"
)

// BulkItemResult is one item's outcome from a bulk operation (FUNC-SPEC §9.4).
type BulkItemResult struct {
	Index   int
	Outcome audit.Outcome
	Error   string
}

// BulkSummary counts a bulk operation's per-item outcomes.
type BulkSummary struct {
	Total     int
	Succeeded int
	Failed    int
}

// BulkResult is a fully-executed bulk operation's outcome (FUNC-SPEC §9.4 step 4).
type BulkResult struct {
	Items   []BulkItemResult
	Summary BulkSummary
}

// BulkValidate runs FUNC-SPEC §9.4 steps 1-2 over items: validate reports a
// non-nil error for any item that fails schema or cheap cluster-fact
// validation. ok is true only when every item passed; failed otherwise
// lists every failing item's BulkItemResult (Outcome always Failed), in
// index order, for a 400 BULK_VALIDATION_FAILED response — nothing has
// executed yet, so no ATTEMPT audit is due.
func BulkValidate[T any](items []T, validate func(T) error) (failed []BulkItemResult, ok bool) {
	ok = true
	for i, item := range items {
		if err := validate(item); err != nil {
			failed = append(failed, BulkItemResult{Index: i, Outcome: audit.OutcomeFailed, Error: err.Error()})
			ok = false
		}
	}
	if !ok {
		return failed, false
	}
	return nil, true
}

// BulkExecute runs execute independently over every item (FUNC-SPEC §9.4
// step 4): one item's failure never stops the others. Call only after
// BulkValidate reports ok, and only between the caller's own ATTEMPT and
// RESULT audit calls — BulkExecute itself is audit-agnostic, so it fits any
// command's own Event construction.
func BulkExecute[T any](ctx context.Context, items []T, execute func(context.Context, T) error) BulkResult {
	results := make([]BulkItemResult, len(items))
	summary := BulkSummary{Total: len(items)}
	for i, item := range items {
		if err := execute(ctx, item); err != nil {
			results[i] = BulkItemResult{Index: i, Outcome: audit.OutcomeFailed, Error: err.Error()}
			summary.Failed++
			continue
		}
		results[i] = BulkItemResult{Index: i, Outcome: audit.OutcomeSucceeded}
		summary.Succeeded++
	}
	return BulkResult{Items: results, Summary: summary}
}

// BulkRun sequences all of FUNC-SPEC §9.4 end to end, including its two-phase
// audit (FUNC-SPEC §8.5): BulkValidate first — any failure returns
// *PolicyError{Code: BulkValidationFailed} naming every failing item, and
// nothing executes, no ATTEMPT. All valid: auditor.Attempt(ctx, attempt) — a
// sink failure fails closed as *PolicyError{Code: AuditUnavailable} (V2),
// again before anything executes — then BulkExecute runs every item
// independently, and auditor.Result(ctx, resultFrom(summary)) reports the
// outcome. attempt and the event resultFrom builds are the caller's own
// (Target, Caller, CommandID, ...) — BulkRun only forces their Phase.
func BulkRun[T any](
	ctx context.Context,
	auditor *audit.Auditor,
	attempt audit.Event,
	resultFrom func(BulkSummary) audit.Event,
	items []T,
	validate func(T) error,
	execute func(context.Context, T) error,
) (BulkResult, error) {
	failed, ok := BulkValidate(items, validate)
	if !ok {
		return BulkResult{}, &PolicyError{
			Code:    BulkValidationFailed,
			Message: bulkValidationMessage(failed),
			Details: map[string]any{"items": failed},
		}
	}

	if err := auditor.Attempt(ctx, attempt); err != nil {
		return BulkResult{}, &PolicyError{Code: AuditUnavailable, Message: err.Error()}
	}

	result := BulkExecute(ctx, items, execute)
	auditor.Result(ctx, resultFrom(result.Summary))
	return result, nil
}

// bulkValidationMessage summarises every failed item into one message for
// *PolicyError.Message; Details.items carries the full per-item list.
func bulkValidationMessage(failed []BulkItemResult) string {
	msg := fmt.Sprintf("%d item(s) failed validation", len(failed))
	if len(failed) > 0 {
		msg += fmt.Sprintf("; first: index %d: %s", failed[0].Index, failed[0].Error)
	}
	return msg
}
