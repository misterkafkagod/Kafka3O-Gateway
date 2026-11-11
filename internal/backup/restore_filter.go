package backup

import (
	"math"
	"time"
)

// RestoreFilter narrows a restore to a time range [From, To) and to some partitions; nil fields mean everything.
type RestoreFilter struct {
	From       *time.Time
	To         *time.Time
	Partitions []int
}

// RecordWindow is a restore's record timestamp range in epoch milliseconds, [From, To).
type RecordWindow struct {
	From int64
	To   int64
}

// AllRecords is the window of a restore without a time range.
var AllRecords = RecordWindow{From: math.MinInt64, To: math.MaxInt64}

// ObjectScan is what one backup object holds for a window: the matching lines and the line number of the last one
// (1-based; 0 when none match), which is the source offset Connect commits once that record is restored.
type ObjectScan struct {
	Matched   int64
	LastMatch int64
}
