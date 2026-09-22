package scan

import "time"

// PartitionSpec is one partition's resolved scan window: read from Start
// (inclusive) up to End (exclusive). End is already clamped to
// min(to, endSnapshot) by the caller (FUNC-SPEC §9.1 rules C9) — reaching
// End on every assigned partition is Exhausted (FUNC-SPEC §9.2).
type PartitionSpec struct {
	Start int64
	End   int64
}

// Spec describes one bounded scan (FUNC-SPEC §9.2). The caller (Task 3.3's
// internal/service/message) resolves `from=`/`to=` into concrete
// per-partition offsets via kafka.Admin before building a Spec — Run itself
// never calls Admin (TECH-SPEC I2: scan holds no port role at all).
type Spec struct {
	Topic      string
	Partitions map[int32]PartitionSpec
	// Latest marks a from=latest scan: continuation reports each
	// partition's Start (not the next unread offset) and ReachedEnd is
	// always true, regardless of how the poll loop actually ended
	// (FUNC-SPEC §9.2, §9.1 rules C9). The caller is responsible for having
	// already resolved Start as max(begin, end-limit) and for sorting
	// emitted items by timestamp descending before truncating to limit.
	Latest bool
	// MaxMessages bounds the number of matched items (0 = unbounded): M1's
	// `limit`, M3/M4's `maxMatches`.
	MaxMessages int
	// MaxScanned bounds the number of records evaluated, matched or not
	// (0 = unbounded): M3/M4's `maxScan`. M1 has no filter, so it never sets
	// this — MaxMessages alone already bounds M1 (scanned == matched there).
	MaxScanned int
	// MaxBytes bounds cumulative scanned key+value bytes (0 = unbounded).
	MaxBytes int64
	// MaxTime bounds wall-clock time from Run's start (0 = unbounded).
	MaxTime time.Duration
	// Format overrides Decode's auto-detected encoding ("" = auto-detect).
	Format string
}
