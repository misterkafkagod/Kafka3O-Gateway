// Package message implements the message commands (FUNC-SPEC §8.7 M1-M8
// minus M8). Service holds kafka.Admin (to resolve `from=`/`to=`, the end
// snapshot, and a topic's partition count), a ConsumerFactory (TECH-SPEC
// §2.3: a dedicated Consumer per scan), and a shared kafka.Producer (TECH-
// SPEC C4: acks=all, idempotent, no dedicated per-call client, unlike
// Consumer) — every role Phase 3/5 actually call (TECH-SPEC I2: "the lists
// are illustrative; the rule governs").
package message

import (
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// ConsumerFactory builds a fresh kafka.Consumer for one scan (TECH-SPEC
// §2.3: a dedicated client per scan). The fake test double may safely
// return the same value every call.
type ConsumerFactory func() (kafka.Consumer, error)

// Range pairs a default with a hard ceiling for one bound.
type Range struct {
	Default int
	Ceiling int
}

// RangeBytes is Range for a byte-size bound.
type RangeBytes struct {
	Default int64
	Ceiling int64
}

// RangeDuration is Range for a time bound.
type RangeDuration struct {
	Default time.Duration
	Ceiling time.Duration
}

// Bounds are the configured defaults and ceilings Read, Search, and Filter
// validate against (FUNC-SPEC §8.8). Limit is M1's own row; MaxScan and
// MaxMatches are M3/M4's; MaxBytes and MaxTime are the single scan-mechanism
// row §8.8 lists once for M3/M4 and that M1 shares, since all three
// ultimately bound the same internal/scan.Run.
type Bounds struct {
	Limit      Range
	MaxScan    Range
	MaxMatches Range
	MaxBytes   RangeBytes
	MaxTime    RangeDuration
	// RegexTimeout bounds each field a Search checks per record (FUNC-SPEC
	// §8.8 "regex per-message match timeout"): fixed, not caller-adjustable
	// (the table gives it a default and no ceiling).
	RegexTimeout time.Duration
	// MaxBulkBodyBytes is M6's request-body ceiling (FUNC-SPEC §8.8: no
	// caller-adjustable default, just a configurable limit) — a body larger
	// than this many bytes reports *core.PolicyError{Code: PayloadTooLarge}
	// before any parsing completes.
	MaxBulkBodyBytes int64
}

// Service implements the message commands.
type Service struct {
	admin       kafka.Admin
	producer    kafka.Producer
	newConsumer ConsumerFactory
	bounds      Bounds
	now         func() time.Time
	// auditor records M5-M7's two-phase ATTEMPT/RESULT audit trail
	// (FUNC-SPEC §8.5, §9.4) — every other message command (M1-M4) is a
	// read and stays unaudited (FUNC-SPEC §8.5 scope).
	auditor    *audit.Auditor
	newEventID func() string
}

// New builds a Service over admin, producer, newConsumer, bounds, and
// auditor.
func New(admin kafka.Admin, producer kafka.Producer, newConsumer ConsumerFactory, bounds Bounds, auditor *audit.Auditor) *Service {
	return &Service{
		admin: admin, producer: producer, newConsumer: newConsumer, bounds: bounds,
		now: time.Now, auditor: auditor, newEventID: audit.NewEventID,
	}
}
