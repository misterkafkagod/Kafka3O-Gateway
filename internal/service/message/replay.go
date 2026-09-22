package message

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// ReplaySource is M8's `source` request field (FUNC-SPEC §8.7 M8). From and
// To reuse M1's own vocabulary and resolution (beginning|latest|offset:<n>|
// timestamp:<ms|iso>) — a resumed call passes the prior response's cursor
// value back in From (e.g. "offset:<cursor>").
type ReplaySource struct {
	Topic string
	// Partitions is the set to copy from; empty means every partition
	// (FUNC-SPEC M8 "partitions?").
	Partitions []int32
	From       From
	To         *From
}

// ReplayTarget is M8's `target` request field (FUNC-SPEC §8.7 M8).
type ReplayTarget struct {
	Topic string
	// PreservePartition keeps each record's original source partition
	// number when producing to Topic, instead of the producer's own
	// key-based partitioning (FUNC-SPEC §9.3 step 3).
	PreservePartition bool
}

// ReplayPlan is M8's dry-run plan (FUNC-SPEC §8.6). ConfirmTarget is the
// target topic name. windows is Apply's own resolved scan window, carried
// through from Plan so Apply never re-resolves it (unexported: not part of
// the dry-run wire shape, which is EstimatedRecords/SourcePartitions/
// TargetPartitions alone).
type ReplayPlan struct {
	Source           ReplaySource
	Target           ReplayTarget
	Limit            int
	EstimatedRecords int64
	SourcePartitions int
	TargetPartitions int
	windows          map[int32]scan.PartitionSpec
}

// ConfirmTarget implements core.Plan.
func (p ReplayPlan) ConfirmTarget() string { return p.Target.Topic }

// ReplayResult is ApplyReplay's output (FUNC-SPEC §8.7 M8).
type ReplayResult struct {
	Copied     int
	Cursor     map[int32]int64
	ReachedEnd bool
}

// PlanReplay resolves source's scan window (reusing resolveWindows, exactly
// as M1's Read does) and target's current partition count (FUNC-SPEC §9.3
// step 1). PreservePartition against a target with too few partitions for
// the highest source partition in scope → *core.PolicyError{Code:
// PartitionMismatch}, before source.Topic's own missing-topic or
// target.Topic's missing-topic errors ever get the chance to differ from
// T2's own NotFound (both come from Admin.DescribeTopics/ListEndOffsets,
// unchanged).
func (s *Service) PlanReplay(ctx context.Context, source ReplaySource, target ReplayTarget, limit int) (ReplayPlan, error) {
	limit, err := resolveIntBound("limit", limit, s.bounds.Replay.Default, s.bounds.Replay.Ceiling)
	if err != nil {
		return ReplayPlan{}, err
	}

	windows, err := s.resolveWindows(ctx, windowParams{
		Topic: source.Topic, Partitions: source.Partitions, From: source.From, To: source.To,
	}, limit)
	if err != nil {
		return ReplayPlan{}, err
	}

	var estimated int64
	var maxSourcePartition int32 = -1
	for p, w := range windows {
		estimated += w.End - w.Start
		if p > maxSourcePartition {
			maxSourcePartition = p
		}
	}

	t, err := s.admin.DescribeTopics(ctx, target.Topic)
	if err != nil {
		return ReplayPlan{}, err
	}
	targetPartitions := len(t.Partitions)

	if target.PreservePartition && int32(targetPartitions) <= maxSourcePartition {
		return ReplayPlan{}, &core.PolicyError{
			Code:    core.PartitionMismatch,
			Message: "target does not have enough partitions to preserve the source's partition numbers",
		}
	}

	return ReplayPlan{
		Source: source, Target: target, Limit: limit,
		EstimatedRecords: estimated, SourcePartitions: len(windows), TargetPartitions: targetPartitions,
		windows: windows,
	}, nil
}

// ApplyReplay consumes plan's source window via internal/scan.Run (TECH-SPEC
// §2.3: the same scan loop M1/M3/M4 use, with no filter) and produces every
// matched record verbatim onto plan.Target.Topic in one Producer.Produce
// call (FUNC-SPEC §9.3 steps 2-3), the same "whole batch, per-item result"
// shape M5/M6 already use. Unlike M5/M6, though, a failure anywhere in the
// batch stops accounting immediately at the first one (FUNC-SPEC §9.3 step
// 4 "mid-batch"): every result at or after that index is treated as not
// copied, even one the broker itself reports as succeeded, since Kafka
// gives no per-record ordering guarantee across a failed batch and the
// caller's retry-from-cursor already tolerates a duplicate (§9.3 step 5,
// at-least-once). The returned error carries copied and cursor so far via
// wrapReplayFailure, for the caller to retry from.
func (s *Service) ApplyReplay(ctx context.Context, plan ReplayPlan) (ReplayResult, error) {
	consumer, err := s.newConsumer()
	if err != nil {
		return ReplayResult{}, err
	}

	spec := scan.Spec{
		Topic: plan.Source.Topic, Partitions: plan.windows,
		Latest: plan.Source.From.Kind == FromLatest, MaxMessages: plan.Limit,
	}
	var items []scan.Record
	stats, err := scan.Run(ctx, consumer, spec, scan.MatchAll, func(r scan.Record) { items = append(items, r) }, s.now)
	if err != nil {
		return ReplayResult{}, err
	}

	cursor := make(map[int32]int64, len(plan.windows))
	for p, w := range plan.windows {
		cursor[p] = w.Start
	}
	if len(items) == 0 {
		return ReplayResult{Cursor: cursor, ReachedEnd: stats.ReachedEnd}, nil
	}

	requests := make([]kafka.ProduceRequest, len(items))
	for i, item := range items {
		req, err := replayProduceRequest(item, plan.Target.PreservePartition)
		if err != nil {
			return ReplayResult{}, wrapReplayFailure(err, 0, cursor)
		}
		requests[i] = req
	}

	results, err := s.producer.Produce(ctx, plan.Target.Topic, requests)
	if err != nil {
		return ReplayResult{}, wrapReplayFailure(err, 0, cursor)
	}

	copied := 0
	for i, r := range results {
		if r.Err != nil {
			return ReplayResult{}, wrapReplayFailure(r.Err, copied, cursor)
		}
		copied++
		cursor[items[i].Partition] = items[i].Offset + 1
	}

	return ReplayResult{Copied: copied, Cursor: cursor, ReachedEnd: stats.ReachedEnd}, nil
}

// replayProduceRequest rebuilds item's original raw bytes via
// scan.EncodeValue (the exact inverse of the Decode that built it), so the
// target record is byte-for-byte identical to the source (FUNC-SPEC §9.3
// step 3 "verbatim"). Partition is set only when preservePartition; nil
// otherwise lets the producer's own key-based partitioner choose.
func replayProduceRequest(item scan.Record, preservePartition bool) (kafka.ProduceRequest, error) {
	key, err := scan.EncodeValue(item.Key, item.KeyEncoding)
	if err != nil {
		return kafka.ProduceRequest{}, err
	}
	value, err := scan.EncodeValue(item.Value, item.ValueEncoding)
	if err != nil {
		return kafka.ProduceRequest{}, err
	}
	headers := make([]kafka.Header, len(item.Headers))
	for i, h := range item.Headers {
		v, err := scan.EncodeValue(h.Value, h.ValueEncoding)
		if err != nil {
			return kafka.ProduceRequest{}, err
		}
		headers[i] = kafka.Header{Key: h.Key, Value: v}
	}

	req := kafka.ProduceRequest{Key: key, Value: value, Headers: headers, Timestamp: item.Timestamp}
	if preservePartition {
		p := item.Partition
		req.Partition = &p
	}
	return req, nil
}

// wrapReplayFailure builds the 502 KAFKA_ERROR *core.PolicyError a mid-batch
// produce failure reports, carrying details.progress{copied,cursor} so the
// caller can retry from exactly where it stopped (FUNC-SPEC §9.3 step 4).
func wrapReplayFailure(cause error, copied int, cursor map[int32]int64) error {
	return &core.PolicyError{
		Code:    core.ReplayFailed,
		Message: cause.Error(),
		Details: map[string]any{"progress": map[string]any{"copied": copied, "cursor": cursor}},
	}
}

// Replay sequences M8's confirm/dryRun/audit lifecycle via core.Destructive
// (FUNC-SPEC §8.6, §9.1; M8 is in §5.6's destructive set and is also
// data-plane, so gates.Check's F6 data-plane-lock check applies here too,
// even on a dry run — CheckAudited runs before the dryRun short-circuit).
func (s *Service) Replay(
	ctx context.Context, caller core.Caller, source ReplaySource, target ReplayTarget, limit int, confirm string, dryRun bool,
) (core.Result[ReplayPlan, ReplayResult], error) {
	desc, _ := command.Lookup("M8")
	attempt := s.newEvent(caller, "M8", target.Topic)
	return core.Destructive(ctx, s.runner, s.auditor, caller, desc, attempt, confirm, dryRun,
		func() (ReplayPlan, error) { return s.PlanReplay(ctx, source, target, limit) },
		func(plan ReplayPlan) (ReplayResult, error) { return s.ApplyReplay(ctx, plan) },
	)
}
