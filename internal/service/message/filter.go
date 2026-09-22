package message

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// FilterParams are Filter's inputs (FUNC-SPEC §8.7 M4, V4): M1's windowing
// params, minus `limit` (not accepted on M3/M4), plus the JSONPath filter
// and the maxScan/maxMatches bounds.
type FilterParams struct {
	Topic      string
	Partitions []int32
	From       From
	To         *From
	Path       string
	Op         scan.FilterOp
	Value      any
	MaxScan    int   // 0 -> Bounds.MaxScan.Default
	MaxMatches int   // 0 -> Bounds.MaxMatches.Default
	MaxBytes   int64 // 0 -> Bounds.MaxBytes.Default
	MaxTimeMs  int64 // 0 -> Bounds.MaxTime.Default
	Format     string
}

// Filter executes one bounded JSONPath filter over p.Topic (FUNC-SPEC §8.7
// M4, V4): resolves `from=`/`to=` and the end snapshot via kafka.Admin, then
// runs internal/scan.Run with a compiled JSONPath Matcher. An invalid path,
// or an invalid regex value for Op == scan.OpRegex, reports
// *core.PolicyError{Code: core.InvalidJSONPath}; maxScan, maxMatches,
// maxBytes, and maxTimeMs above their configured ceiling report
// *core.PolicyError{Code: core.BoundExceeded} (FUNC-SPEC §8.8). A record
// whose value is not JSON, or whose selected node's type does not match Op,
// is skipped or a plain non-match respectively — never an error (FUNC-SPEC
// §9.2 Evaluate).
func (s *Service) Filter(ctx context.Context, p FilterParams) (ReadResult, error) {
	maxScan, maxMatches, maxBytes, maxTime, err := s.resolveScanBounds(p.MaxScan, p.MaxMatches, p.MaxBytes, p.MaxTimeMs)
	if err != nil {
		return ReadResult{}, err
	}

	matcher, err := scan.NewJSONPathMatcher(p.Path, p.Op, p.Value)
	if err != nil {
		return ReadResult{}, &core.PolicyError{Code: core.InvalidJSONPath, Message: err.Error()}
	}

	partitionSpecs, err := s.resolveWindows(ctx, windowParams{
		Topic: p.Topic, Partitions: p.Partitions, From: p.From, To: p.To,
	}, maxMatches)
	if err != nil {
		return ReadResult{}, err
	}

	spec := scan.Spec{
		Topic: p.Topic, Partitions: partitionSpecs, Latest: p.From.Kind == FromLatest,
		MaxMessages: maxMatches, MaxScanned: maxScan, MaxBytes: maxBytes, MaxTime: maxTime, Format: p.Format,
	}
	return s.runScan(ctx, spec, matcher)
}
