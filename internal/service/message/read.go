package message

import (
	"context"

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
	To         *From
	Limit      int   // 0 -> Bounds.Limit.Default
	MaxBytes   int64 // 0 -> Bounds.MaxBytes.Default
	MaxTimeMs  int64 // 0 -> Bounds.MaxTime.Default
	Format     string
}

// ReadResult is Read's (and Search's and Filter's) output (FUNC-SPEC §8.3
// scan envelope).
type ReadResult struct {
	Items []scan.Record
	Stats scan.Stats
}

// Read executes one bounded read over p.Topic (FUNC-SPEC §8.7 M1): resolves
// `from=`/`to=` and the end snapshot via kafka.Admin, then runs
// internal/scan.Run with a fresh, dedicated Consumer (TECH-SPEC §2.3).
// limit, maxBytes, and maxTimeMs above their configured ceiling report
// *core.PolicyError{Code: core.BoundExceeded} (FUNC-SPEC §8.8). Under the
// data-plane lock (FUNC-SPEC §9.5), only an operator caller presenting a
// break-glass reason passes checkGate — which then reports a single HIGH
// RESULT audit event on success, the one case a read is ever audited.
func (s *Service) Read(ctx context.Context, caller core.Caller, p ReadParams) (ReadResult, error) {
	if err := s.checkGate(ctx, caller, "M1", p.Topic); err != nil {
		return ReadResult{}, err
	}

	limit, err := resolveIntBound("limit", p.Limit, s.bounds.Limit.Default, s.bounds.Limit.Ceiling)
	if err != nil {
		return ReadResult{}, err
	}
	maxBytes, err := resolveBytesBound("maxBytes", p.MaxBytes, s.bounds.MaxBytes.Default, s.bounds.MaxBytes.Ceiling)
	if err != nil {
		return ReadResult{}, err
	}
	maxTime, err := resolveMaxTime(p.MaxTimeMs, s.bounds.MaxTime.Default, s.bounds.MaxTime.Ceiling)
	if err != nil {
		return ReadResult{}, err
	}

	partitionSpecs, err := s.resolveWindows(ctx, windowParams{
		Topic: p.Topic, Partitions: p.Partitions, From: p.From, To: p.To,
	}, limit)
	if err != nil {
		return ReadResult{}, err
	}

	spec := scan.Spec{
		Topic: p.Topic, Partitions: partitionSpecs, Latest: p.From.Kind == FromLatest,
		MaxMessages: limit, MaxBytes: maxBytes, MaxTime: maxTime, Format: p.Format,
	}
	result, err := s.runScan(ctx, spec, scan.MatchAll)
	if err != nil {
		return ReadResult{}, err
	}
	s.auditBreakGlassRead(ctx, caller, "M1", p.Topic)
	return result, nil
}
