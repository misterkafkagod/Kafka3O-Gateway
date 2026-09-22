package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// SlogSink writes each Event as structured JSON via logger (FUNC-SPEC F5).
// It marshals ev itself (encoding/json.RawMessage, which slog's JSON handler
// emits verbatim) rather than logging its fields as separate slog
// attributes, so the emitted "event" value's keys are always exactly Event's
// own json tags — never dependent on how slog encodes an arbitrary struct.
type SlogSink struct {
	logger *slog.Logger
}

// NewSlogSink builds a SlogSink over logger.
func NewSlogSink(logger *slog.Logger) *SlogSink {
	return &SlogSink{logger: logger}
}

// Write logs ev as one structured line.
func (s *SlogSink) Write(ctx context.Context, ev Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("audit: marshal event: %w", err)
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "audit event", slog.Any("event", json.RawMessage(data)))
	return nil
}

// compile-time proof that SlogSink satisfies Sink.
var _ Sink = (*SlogSink)(nil)
