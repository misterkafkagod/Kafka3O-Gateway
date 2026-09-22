// Package message implements the message-reading commands (FUNC-SPEC §8.7
// M1, M2). Service holds kafka.Admin (to resolve `from=`/`to=` and the end
// snapshot) and a ConsumerFactory (TECH-SPEC §2.3: a dedicated Consumer per
// scan) — not kafka.Producer, which arrives with M5-M8 in Phase 5
// (TECH-SPEC I2: "the lists are illustrative; the rule governs" — a service
// holds only the roles it currently calls).
package message

import (
	"time"

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
}

// Service implements the message commands.
type Service struct {
	admin       kafka.Admin
	newConsumer ConsumerFactory
	bounds      Bounds
	now         func() time.Time
}

// New builds a Service over admin, newConsumer, and bounds.
func New(admin kafka.Admin, newConsumer ConsumerFactory, bounds Bounds) *Service {
	return &Service{admin: admin, newConsumer: newConsumer, bounds: bounds, now: time.Now}
}
