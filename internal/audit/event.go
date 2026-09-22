// Package audit implements the two-phase audit event schema and sinks
// (FUNC-SPEC §8.5, F5). It depends only on the standard library and
// internal/kafka (the port, for the Kafka sink's Producer) — never
// internal/service/core, so building an Event from a request's Caller is
// the caller's own job, done where both audit and service/core are already
// in scope (internal/service, Task 5.3).
package audit

import "time"

// Phase identifies where in the two-phase audit sequence an Event sits
// (FUNC-SPEC §8.5).
type Phase string

// Phase values.
const (
	PhaseAttempt Phase = "ATTEMPT"
	PhaseResult  Phase = "RESULT"
)

// Severity classifies an Event (FUNC-SPEC §8.5 V6).
type Severity string

// Severity values.
const (
	SeverityInfo Severity = "INFO"
	SeverityWarn Severity = "WARN"
	SeverityHigh Severity = "HIGH"
)

// Outcome is a RESULT event's disposition (FUNC-SPEC §8.5).
type Outcome string

// Outcome values.
const (
	OutcomeSucceeded Outcome = "SUCCEEDED"
	OutcomeFailed    Outcome = "FAILED"
	OutcomeRejected  Outcome = "REJECTED"
)

// Target names what an event's command acted on (FUNC-SPEC §8.5).
type Target struct {
	Type       string  `json:"type"`
	Name       string  `json:"name"`
	Partitions []int32 `json:"partitions,omitempty"`
}

// Caller identifies who made the request (FUNC-SPEC §8.5). KeyID is nil on a
// 401 rejection or with authentication disabled (TECH-SPEC C14).
type Caller struct {
	KeyID    *string `json:"keyId"`
	Tier     string  `json:"tier"`
	ClientIP string  `json:"clientIp"`
}

// BreakGlass records a non-empty X-Break-Glass-Reason (FUNC-SPEC §8.2 C2:
// capped at 512 bytes, control characters stripped).
type BreakGlass struct {
	Reason string `json:"reason"`
}

// EventError carries a RESULT event's failure code (FUNC-SPEC §8.5).
type EventError struct {
	Code string `json:"code"`
}

// Event is exactly the FUNC-SPEC §8.5 audit event schema. Unlike
// internal/kafka's domain types (TECH-SPEC I5), Event carries JSON tags
// directly: it is itself the wire format written to stdout and the audit
// topic, with no separate DTO layer.
type Event struct {
	EventID     string      `json:"eventId"`
	Timestamp   time.Time   `json:"timestamp"`
	RequestID   string      `json:"requestId"`
	Phase       Phase       `json:"phase"`
	Severity    Severity    `json:"severity"`
	CommandID   string      `json:"commandId"`
	CommandName string      `json:"commandName"`
	Target      Target      `json:"target"`
	Caller      Caller      `json:"caller"`
	DryRun      bool        `json:"dryRun"`
	BreakGlass  *BreakGlass `json:"breakGlass,omitempty"`
	Outcome     Outcome     `json:"outcome,omitempty"`
	Error       *EventError `json:"error,omitempty"`
	DurationMs  *int64      `json:"durationMs,omitempty"`
}

// highSeverityCommands are commands whose successful execution is always
// HIGH severity (FUNC-SPEC §8.5 V6), regardless of break-glass.
var highSeverityCommands = map[string]bool{
	"T7": true, "T8": true, "T11": true, "T12": true, "G5": true,
}

// SeverityFor computes an event's severity (FUNC-SPEC §8.5 V6): HIGH for any
// break-glass use (attempted or granted) or a successful T7/T8/T11/T12/G5;
// WARN for a rejected or failed outcome; INFO for a successful execution or
// dry-run.
func SeverityFor(commandID string, outcome Outcome, breakGlass bool) Severity {
	switch {
	case breakGlass:
		return SeverityHigh
	case outcome == OutcomeRejected || outcome == OutcomeFailed:
		return SeverityWarn
	case outcome == OutcomeSucceeded && highSeverityCommands[commandID]:
		return SeverityHigh
	default:
		return SeverityInfo
	}
}
