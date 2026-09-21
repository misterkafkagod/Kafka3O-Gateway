package scan

// StoppedBy values (FUNC-SPEC §8.3 scan envelope). The zero value ""
// represents the envelope's `stoppedBy: null` — the scan reached its
// (possibly `to`-clamped) end snapshot rather than hitting a bound.
const (
	StoppedByMaxMessages = "maxMessages"
	StoppedByMaxBytes    = "maxBytes"
	StoppedByMaxTime     = "maxTime"
)

// Stats is one scan's statistics and continuation (FUNC-SPEC §8.3 scan
// envelope).
type Stats struct {
	Scanned    int
	Matched    int
	Skipped    int
	Bytes      int64
	ElapsedMs  int64
	ReachedEnd bool
	StoppedBy  string
	// Continuation is the next offset to read per partition, so the caller
	// can resume with no gap and no overlap (FUNC-SPEC §9.2). For a
	// from=latest scan this is each partition's resolved start offset
	// instead, with ReachedEnd always true (FUNC-SPEC §9.1 rules C9).
	Continuation map[int32]int64
}
