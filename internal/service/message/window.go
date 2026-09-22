package message

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/scan"
)

// windowParams are the `from=`/`to=`/partition-selection parameters M1, M3,
// and M4 all share (FUNC-SPEC §8.7 M1 params, reused by M3/M4).
type windowParams struct {
	Topic      string
	Partitions []int32
	From       From
	// To is optional; nil means read to the end snapshot (FUNC-SPEC M1
	// "to?"; §9.1 rules C9: to = min(to, endSnapshot)).
	To *From
}

// resolveWindows resolves wp into a concrete per-partition scan window,
// calling kafka.Admin only for what From/To actually need. limitForLatest is
// the matched-item limit a from=latest window sizes itself to (start =
// max(begin, end-limit)); it is ignored for every other From.Kind.
func (s *Service) resolveWindows(ctx context.Context, wp windowParams, limitForLatest int) (map[int32]scan.PartitionSpec, error) {
	endOffsets, err := s.admin.ListEndOffsets(ctx, wp.Topic)
	if err != nil {
		return nil, err
	}

	partitions := wp.Partitions
	if len(partitions) == 0 {
		partitions = make([]int32, 0, len(endOffsets))
		for part := range endOffsets {
			partitions = append(partitions, part)
		}
	}

	beginOffsets, err := s.maybeListStart(ctx, wp.Topic, wp.From.Kind)
	if err != nil {
		return nil, err
	}
	fromTimestampOffsets, err := s.maybeListAfterMilli(ctx, wp.Topic, wp.From)
	if err != nil {
		return nil, err
	}
	var toTimestampOffsets map[int32]int64
	if wp.To != nil {
		toTimestampOffsets, err = s.maybeListAfterMilli(ctx, wp.Topic, *wp.To)
		if err != nil {
			return nil, err
		}
	}

	partitionSpecs := make(map[int32]scan.PartitionSpec, len(partitions))
	for _, part := range partitions {
		end := endOffsets[part]
		start := resolveFromOffset(wp.From, part, beginOffsets, fromTimestampOffsets, end, limitForLatest)
		if wp.To != nil {
			if to := resolveToOffset(*wp.To, part, toTimestampOffsets, end); to < end {
				end = to
			}
		}
		partitionSpecs[part] = scan.PartitionSpec{Start: start, End: end}
	}
	return partitionSpecs, nil
}

// maybeListStart calls ListStartOffsets only when kind actually needs begin
// offsets (FromBeginning, or FromLatest's max(begin, end-limit)).
func (s *Service) maybeListStart(ctx context.Context, topic string, kind FromKind) (map[int32]int64, error) {
	if kind != FromBeginning && kind != FromLatest {
		return nil, nil
	}
	return s.admin.ListStartOffsets(ctx, topic)
}

// maybeListAfterMilli calls ListOffsetsAfterMilli only when from is a
// timestamp form.
func (s *Service) maybeListAfterMilli(ctx context.Context, topic string, from From) (map[int32]int64, error) {
	if from.Kind != FromTimestamp {
		return nil, nil
	}
	return s.admin.ListOffsetsAfterMilli(ctx, topic, from.TimeMilli)
}

// resolveFromOffset resolves one partition's start offset per from.Kind
// (FUNC-SPEC §8.7 M1, §9.2 "latest": start = max(begin, end-limit)).
func resolveFromOffset(from From, part int32, begin, timestampOffsets map[int32]int64, end int64, limit int) int64 {
	switch from.Kind {
	case FromBeginning:
		return begin[part]
	case FromLatest:
		start := end - int64(limit)
		if b := begin[part]; start < b {
			start = b
		}
		return start
	case FromOffset:
		return from.Offset
	case FromTimestamp:
		return timestampOffsets[part]
	default:
		return begin[part]
	}
}

// resolveToOffset resolves one partition's upper bound per to.Kind, falling
// back to the end snapshot for FromBeginning/FromLatest (neither is a
// meaningful `to`).
func resolveToOffset(to From, part int32, timestampOffsets map[int32]int64, end int64) int64 {
	switch to.Kind {
	case FromOffset:
		return to.Offset
	case FromTimestamp:
		return timestampOffsets[part]
	default:
		return end
	}
}

// runScan builds a fresh Consumer and executes spec against it with matcher
// (TECH-SPEC §2.3: a dedicated client per scan).
func (s *Service) runScan(ctx context.Context, spec scan.Spec, matcher scan.Matcher) (ReadResult, error) {
	consumer, err := s.newConsumer()
	if err != nil {
		return ReadResult{}, err
	}

	var items []scan.Record
	stats, err := scan.Run(ctx, consumer, spec, matcher, func(r scan.Record) { items = append(items, r) }, s.now)
	if err != nil {
		return ReadResult{}, err
	}
	return ReadResult{Items: items, Stats: stats}, nil
}
