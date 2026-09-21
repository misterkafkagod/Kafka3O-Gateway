package scan

import (
	"context"
	"sort"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Emit is called once per matched record, in the order the caller should
// present it, so it can accumulate the scan envelope's items (FUNC-SPEC §8.3).
type Emit func(Record)

// Run executes one bounded scan against consumer, following FUNC-SPEC §9.2's
// state machine: Resolve -> Assign -> Poll -> Evaluate ->
// Bounded/Exhausted/Empty -> Done. now supplies the wall clock MaxTime
// measures from (TECH-SPEC §4.9: injected for tests). Run assigns consumer
// itself from spec's resolved per-partition start offsets, and closes it
// before returning (TECH-SPEC §2.3: a dedicated client per scan).
//
// "No partitions in scope" (FUNC-SPEC §9.2 Resolve -> Empty) is the only
// case Run special-cases ahead of Assign; "every from >= end" produces the
// identical observable result (items = [], reachedEnd = true, stoppedBy =
// "") by simply never finding anything to poll, so it needs no separate
// branch.
//
// For spec.Latest, MaxMessages never stops the poll loop early — the caller
// already sized each partition's window to roughly MaxMessages records via
// start = max(begin, end-limit), but a scan across several partitions can
// still gather more than MaxMessages candidates in total, so Run buffers
// every match, then sorts by timestamp descending and truncates to
// MaxMessages before emitting (FUNC-SPEC §9.2 "latest").
func Run(ctx context.Context, consumer kafka.Consumer, spec Spec, matcher Matcher, emit Emit, now func() time.Time) (Stats, error) {
	start := now()

	cursor := make(map[int32]int64, len(spec.Partitions))
	for p, ps := range spec.Partitions {
		cursor[p] = ps.Start
	}
	if len(spec.Partitions) == 0 {
		return finalStats(spec, cursor, true, "", 0, &accumulator{}), nil
	}

	partitions := make([]int32, 0, len(spec.Partitions))
	startOffsets := make(map[int32]int64, len(spec.Partitions))
	for p, ps := range spec.Partitions {
		partitions = append(partitions, p)
		startOffsets[p] = ps.Start
	}
	if err := consumer.Assign(ctx, spec.Topic, partitions, startOffsets); err != nil {
		return Stats{}, err
	}
	defer consumer.Close()

	// maxTime is wall-clock from here, via a real context timeout — never
	// the injected now, which only makes ElapsedMs deterministic for tests
	// (TECH-SPEC §4.9; TASKS.md Task 3.2.1: "context.WithTimeout(maxTime)").
	pollCtx := ctx
	if spec.MaxTime > 0 {
		var cancel context.CancelFunc
		pollCtx, cancel = context.WithTimeout(ctx, spec.MaxTime)
		defer cancel()
	}

	acc := &accumulator{}
	collect, drain := emitter(spec, emit)
	reachedEnd, stoppedBy, err := pollLoop(pollCtx, consumer, spec, cursor, matcher, collect, acc)
	if err != nil {
		return Stats{}, err
	}
	drain(acc)

	elapsed := now().Sub(start).Milliseconds()
	return finalStats(spec, cursor, reachedEnd, stoppedBy, elapsed, acc), nil
}

// emitter returns the Emit pollLoop should call and a drain step run once
// polling finishes. For an ordinary scan, collect is emit itself and drain
// is a no-op. For spec.Latest, collect buffers instead, and drain sorts the
// buffer by timestamp descending, truncates to MaxMessages, calls emit for
// each survivor, and corrects acc.matched to the truncated count so
// Stats.Matched agrees with what was actually emitted.
func emitter(spec Spec, emit Emit) (collect Emit, drain func(*accumulator)) {
	if !spec.Latest {
		return emit, func(*accumulator) {}
	}
	var buffer []Record
	collect = func(r Record) { buffer = append(buffer, r) }
	drain = func(acc *accumulator) {
		sort.Slice(buffer, func(i, j int) bool { return buffer[i].Timestamp.After(buffer[j].Timestamp) })
		if spec.MaxMessages > 0 && len(buffer) > spec.MaxMessages {
			buffer = buffer[:spec.MaxMessages]
		}
		acc.matched = len(buffer)
		for _, r := range buffer {
			emit(r)
		}
	}
	return collect, drain
}

// accumulator carries pollLoop's running counts; Stats itself is assembled
// once, at the end, from these plus the final cursor.
type accumulator struct {
	scanned, matched, skipped int
	bytes                     int64
}

// pollLoop runs Poll/Evaluate until every assigned partition's cursor
// reaches its End (Exhausted), a bound is hit (Bounded), or pollCtx ends
// (reported as the maxTime bound) — FUNC-SPEC §9.2.
func pollLoop(pollCtx context.Context, consumer kafka.Consumer, spec Spec, cursor map[int32]int64, matcher Matcher, emit Emit, acc *accumulator) (reachedEnd bool, stoppedBy string, err error) {
	for {
		if windowsExhausted(spec, cursor) {
			return true, "", nil
		}

		records, pollErr := consumer.Poll(pollCtx)
		if pollErr != nil {
			if pollCtx.Err() != nil {
				return false, StoppedByMaxTime, nil
			}
			return false, "", pollErr
		}

		for _, r := range records {
			counted, bound := evaluate(r, spec, cursor, matcher, emit, acc)
			if counted && bound != "" {
				return false, bound, nil
			}
		}

		if len(records) == 0 && pollCtx.Err() != nil {
			return false, StoppedByMaxTime, nil
		}
	}
}

// evaluate processes one polled record against spec's window: records
// outside [cursor, End) are stale or ran past the end snapshot (a hot topic
// mid-scan) and are silently ignored, never counted (FUNC-SPEC §9.2 "End
// snapshot"). counted reports whether r fell inside the window at all, so
// pollLoop only checks bound after a record that actually counted.
func evaluate(r kafka.Record, spec Spec, cursor map[int32]int64, matcher Matcher, emit Emit, acc *accumulator) (counted bool, bound string) {
	window, ok := spec.Partitions[r.Partition]
	if !ok || r.Offset < cursor[r.Partition] || r.Offset >= window.End {
		return false, ""
	}
	cursor[r.Partition] = r.Offset + 1

	acc.scanned++
	acc.bytes += int64(len(r.Key) + len(r.Value))

	decoded := Decode(r, spec.Format)
	result := matcher(decoded)
	switch {
	case result.Skipped:
		acc.skipped++
	case result.Match:
		acc.matched++
		emit(decoded)
	}

	return true, hitBound(acc, spec)
}

// windowsExhausted reports whether every partition's cursor has reached its End.
func windowsExhausted(spec Spec, cursor map[int32]int64) bool {
	for p, ps := range spec.Partitions {
		if cursor[p] < ps.End {
			return false
		}
	}
	return true
}

// hitBound reports which bound acc has just reached, if any (FUNC-SPEC §8.8,
// §9.2). MaxMessages bounds matched items and never stops a Latest scan
// early (Run truncates it globally afterward instead); MaxBytes bounds
// cumulative scanned key+value bytes, regardless of match, in every mode.
func hitBound(acc *accumulator, spec Spec) string {
	if !spec.Latest && spec.MaxMessages > 0 && acc.matched >= spec.MaxMessages {
		return StoppedByMaxMessages
	}
	if spec.MaxBytes > 0 && acc.bytes >= spec.MaxBytes {
		return StoppedByMaxBytes
	}
	return ""
}

// finalStats assembles Stats, applying from=latest's continuation/reachedEnd
// override (FUNC-SPEC §9.1 rules C9).
func finalStats(spec Spec, cursor map[int32]int64, reachedEnd bool, stoppedBy string, elapsedMs int64, acc *accumulator) Stats {
	continuation := make(map[int32]int64, len(spec.Partitions))
	for p, ps := range spec.Partitions {
		if spec.Latest {
			continuation[p] = ps.Start
		} else {
			continuation[p] = cursor[p]
		}
	}
	if spec.Latest {
		reachedEnd, stoppedBy = true, ""
	}
	return Stats{
		Scanned: acc.scanned, Matched: acc.matched, Skipped: acc.skipped,
		Bytes: acc.bytes, ElapsedMs: elapsedMs,
		ReachedEnd: reachedEnd, StoppedBy: stoppedBy, Continuation: continuation,
	}
}
