package message

import (
	"context"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// FromKind identifies how a read's starting (or, for To, ending) point was
// specified (FUNC-SPEC §8.7 M1).
type FromKind int

// FromKind values.
const (
	FromBeginning FromKind = iota
	FromLatest
	FromOffset
	FromTimestamp
)

// From is M1's `from=` (or `to=`) parameter, already parsed from its wire
// syntax — beginning|latest|offset:<n>|timestamp:<ms|iso> — by the API layer
// (Task 3.4); Read resolves it into a concrete offset via kafka.Admin.
type From struct {
	Kind      FromKind
	Offset    int64 // meaningful when Kind == FromOffset
	TimeMilli int64 // meaningful when Kind == FromTimestamp
}

// ReadParams are Read's inputs (FUNC-SPEC §8.7 M1).
type ReadParams struct {
	Topic string
	// Partitions is the set to read; empty means every partition (FUNC-SPEC
	// M1 "partition? (omit = all)").
	Partitions []int32
	From       From
	// To is M1's optional upper bound; nil means read to the end snapshot
	// (FUNC-SPEC M1 "to?"; §9.1 rules C9: to = min(to, endSnapshot)).
	To        *From
	Limit     int   // 0 -> Bounds.Limit.Default
	MaxBytes  int64 // 0 -> Bounds.MaxBytes.Default
	MaxTimeMs int64 // 0 -> Bounds.MaxTime.Default
	Format    string
}

// ReadResult is Read's output (FUNC-SPEC §8.3 scan envelope).
type ReadResult struct {
	Items []scan.Record
	Stats scan.Stats
}

// Read executes one bounded read over p.Topic (FUNC-SPEC §8.7 M1): resolves
// `from=`/`to=` and the end snapshot via kafka.Admin, then runs
// internal/scan.Run with a fresh, dedicated Consumer (TECH-SPEC §2.3).
// limit, maxBytes, and maxTimeMs above their configured ceiling report
// *core.PolicyError{Code: core.BoundExceeded} (FUNC-SPEC §8.8).
func (s *Service) Read(ctx context.Context, p ReadParams) (ReadResult, error) {
	limit, maxBytes, maxTime, err := s.resolveBounds(p)
	if err != nil {
		return ReadResult{}, err
	}

	endOffsets, err := s.admin.ListEndOffsets(ctx, p.Topic)
	if err != nil {
		return ReadResult{}, err
	}

	partitions := p.Partitions
	if len(partitions) == 0 {
		partitions = make([]int32, 0, len(endOffsets))
		for part := range endOffsets {
			partitions = append(partitions, part)
		}
	}

	beginOffsets, err := s.maybeListStart(ctx, p.Topic, p.From.Kind)
	if err != nil {
		return ReadResult{}, err
	}
	fromTimestampOffsets, err := s.maybeListAfterMilli(ctx, p.Topic, p.From)
	if err != nil {
		return ReadResult{}, err
	}
	var toTimestampOffsets map[int32]int64
	if p.To != nil {
		toTimestampOffsets, err = s.maybeListAfterMilli(ctx, p.Topic, *p.To)
		if err != nil {
			return ReadResult{}, err
		}
	}

	partitionSpecs := make(map[int32]scan.PartitionSpec, len(partitions))
	for _, part := range partitions {
		end := endOffsets[part]
		start := resolveFromOffset(p.From, part, beginOffsets, fromTimestampOffsets, end, limit)
		if p.To != nil {
			if to := resolveToOffset(*p.To, part, toTimestampOffsets, end); to < end {
				end = to
			}
		}
		partitionSpecs[part] = scan.PartitionSpec{Start: start, End: end}
	}

	spec := scan.Spec{
		Topic: p.Topic, Partitions: partitionSpecs, Latest: p.From.Kind == FromLatest,
		MaxMessages: limit, MaxBytes: maxBytes, MaxTime: maxTime, Format: p.Format,
	}

	consumer, err := s.newConsumer()
	if err != nil {
		return ReadResult{}, err
	}

	var items []scan.Record
	stats, err := scan.Run(ctx, consumer, spec, scan.MatchAll, func(r scan.Record) { items = append(items, r) }, s.now)
	if err != nil {
		return ReadResult{}, err
	}
	return ReadResult{Items: items, Stats: stats}, nil
}

// resolveBounds applies each bound's default when the caller left it zero,
// and reports BoundExceeded for anything above its configured ceiling
// (FUNC-SPEC §8.8).
func (s *Service) resolveBounds(p ReadParams) (limit int, maxBytes int64, maxTime time.Duration, err error) {
	limit = p.Limit
	if limit == 0 {
		limit = s.bounds.Limit.Default
	} else if limit > s.bounds.Limit.Ceiling {
		return 0, 0, 0, &core.PolicyError{Code: core.BoundExceeded, Message: "limit exceeds the configured ceiling"}
	}

	maxBytes = p.MaxBytes
	if maxBytes == 0 {
		maxBytes = s.bounds.MaxBytes.Default
	} else if maxBytes > s.bounds.MaxBytes.Ceiling {
		return 0, 0, 0, &core.PolicyError{Code: core.BoundExceeded, Message: "maxBytes exceeds the configured ceiling"}
	}

	maxTimeMs := p.MaxTimeMs
	if maxTimeMs == 0 {
		maxTime = s.bounds.MaxTime.Default
	} else {
		maxTime = time.Duration(maxTimeMs) * time.Millisecond
		if maxTime > s.bounds.MaxTime.Ceiling {
			return 0, 0, 0, &core.PolicyError{Code: core.BoundExceeded, Message: "maxTimeMs exceeds the configured ceiling"}
		}
	}
	return limit, maxBytes, maxTime, nil
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
