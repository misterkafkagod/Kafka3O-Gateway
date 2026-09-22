package message

import (
	"context"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// SearchParams are Search's inputs (FUNC-SPEC §8.7 M3): M1's windowing
// params, minus `limit` (not accepted on M3/M4), plus regex and the
// maxScan/maxMatches bounds.
type SearchParams struct {
	Topic           string
	Partitions      []int32
	From            From
	To              *From
	Regex           string
	Fields          []scan.RegexField
	CaseInsensitive bool
	MaxScan         int   // 0 -> Bounds.MaxScan.Default
	MaxMatches      int   // 0 -> Bounds.MaxMatches.Default
	MaxBytes        int64 // 0 -> Bounds.MaxBytes.Default
	MaxTimeMs       int64 // 0 -> Bounds.MaxTime.Default
	Format          string
}

// Search executes one bounded regex search over p.Topic (FUNC-SPEC §8.7 M3):
// resolves `from=`/`to=` and the end snapshot via kafka.Admin, then runs
// internal/scan.Run with a compiled regex Matcher. An invalid pattern
// reports *core.PolicyError{Code: core.InvalidRegex}; maxScan, maxMatches,
// maxBytes, and maxTimeMs above their configured ceiling report
// *core.PolicyError{Code: core.BoundExceeded} (FUNC-SPEC §8.8).
func (s *Service) Search(ctx context.Context, p SearchParams) (ReadResult, error) {
	maxScan, maxMatches, maxBytes, maxTime, err := s.resolveScanBounds(p.MaxScan, p.MaxMatches, p.MaxBytes, p.MaxTimeMs)
	if err != nil {
		return ReadResult{}, err
	}

	matcher, err := scan.NewRegexMatcher(p.Regex, p.Fields, p.CaseInsensitive, s.bounds.RegexTimeout)
	if err != nil {
		return ReadResult{}, &core.PolicyError{Code: core.InvalidRegex, Message: err.Error()}
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

// resolveScanBounds applies M3/M4's shared bound resolution: maxScan and
// maxMatches each default and cap independently, then maxBytes/maxTimeMs as
// Read does (FUNC-SPEC §8.8).
func (s *Service) resolveScanBounds(maxScan, maxMatches int, maxBytesIn, maxTimeMsIn int64) (scanBound, matchBound int, maxBytes int64, maxTime time.Duration, err error) {
	scanBound, err = resolveIntBound("maxScan", maxScan, s.bounds.MaxScan.Default, s.bounds.MaxScan.Ceiling)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	matchBound, err = resolveIntBound("maxMatches", maxMatches, s.bounds.MaxMatches.Default, s.bounds.MaxMatches.Ceiling)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	maxBytes, err = resolveBytesBound("maxBytes", maxBytesIn, s.bounds.MaxBytes.Default, s.bounds.MaxBytes.Ceiling)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	maxTime, err = resolveMaxTime(maxTimeMsIn, s.bounds.MaxTime.Default, s.bounds.MaxTime.Ceiling)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return scanBound, matchBound, maxBytes, maxTime, nil
}
