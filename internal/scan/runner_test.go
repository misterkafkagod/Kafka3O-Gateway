package scan_test

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/scan"
)

// fixedNow is the injected clock for every test that doesn't itself care
// about real elapsed time (TECH-SPEC §4.9): it never advances, so
// Stats.ElapsedMs is always exactly 0 for those tests.
func fixedNow() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

func collector() (scan.Emit, *[]scan.Record) {
	items := &[]scan.Record{}
	return func(r scan.Record) { *items = append(*items, r) }, items
}

func TestScanRun_Empty_NoPartitionsOrFromAtEnd(t *testing.T) {
	t.Parallel()

	t.Run("no partitions in scope", func(t *testing.T) {
		t.Parallel()
		f := fake.New()
		emit, items := collector()
		stats, err := scan.Run(context.Background(), f, scan.Spec{Topic: "t"}, scan.MatchAll, emit, fixedNow)
		if err != nil {
			t.Fatalf("Run() error: %v", err)
		}
		if len(*items) != 0 || !stats.ReachedEnd || stats.StoppedBy != "" {
			t.Fatalf("Stats = %+v, items = %v, want empty/reachedEnd/no stoppedBy", stats, *items)
		}
	})

	t.Run("from already at end", func(t *testing.T) {
		t.Parallel()
		f := fake.New()
		f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
		spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 1, End: 1}}}
		emit, items := collector()
		stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
		if err != nil {
			t.Fatalf("Run() error: %v", err)
		}
		if len(*items) != 0 || !stats.ReachedEnd || stats.StoppedBy != "" {
			t.Fatalf("Stats = %+v, items = %v, want empty/reachedEnd/no stoppedBy", stats, *items)
		}
		if stats.Continuation[0] != 1 {
			t.Errorf("Continuation[0] = %d, want 1 (unchanged Start)", stats.Continuation[0])
		}
	})
}

func TestScanRun_Exhausted_ReachedEndTrueStoppedByNil(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 3}}}
	emit, items := collector()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(*items) != 3 || !stats.ReachedEnd || stats.StoppedBy != "" {
		t.Fatalf("Stats = %+v, items = %d, want 3 items, reachedEnd, no stoppedBy", stats, len(*items))
	}
	if stats.Continuation[0] != 3 {
		t.Errorf("Continuation[0] = %d, want 3", stats.Continuation[0])
	}
	if stats.Scanned != 3 || stats.Matched != 3 {
		t.Errorf("Scanned/Matched = %d/%d, want 3/3", stats.Scanned, stats.Matched)
	}
}

func TestScanRun_Bounded_MaxMessages(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 3}}, MaxMessages: 2}
	emit, items := collector()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(*items) != 2 || stats.ReachedEnd || stats.StoppedBy != scan.StoppedByMaxMessages {
		t.Fatalf("Stats = %+v, items = %d, want 2 items, not reachedEnd, stoppedBy maxMessages", stats, len(*items))
	}
	if stats.Continuation[0] != 2 {
		t.Errorf("Continuation[0] = %d, want 2", stats.Continuation[0])
	}
}

func TestScanRun_Bounded_MaxBytes(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("aaaaa")},
		kafka.Record{Partition: 0, Value: []byte("bbbbb")},
		kafka.Record{Partition: 0, Value: []byte("ccccc")},
	)
	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 3}}, MaxBytes: 8}
	emit, items := collector()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	// 5 bytes after record 1 (< 8, continue), 10 bytes after record 2 (>= 8, stop).
	if len(*items) != 2 || stats.ReachedEnd || stats.StoppedBy != scan.StoppedByMaxBytes {
		t.Fatalf("Stats = %+v, items = %d, want 2 items, stoppedBy maxBytes", stats, len(*items))
	}
	if stats.Bytes != 10 {
		t.Errorf("Bytes = %d, want 10", stats.Bytes)
	}
}

func TestScanRun_Bounded_MaxTime_FixedClockWithLatency(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	f.Latency("Poll", 200*time.Millisecond)

	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 1}}, MaxTime: 20 * time.Millisecond}
	start := time.Now()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, func(scan.Record) {}, fixedNow)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if stats.ReachedEnd || stats.StoppedBy != scan.StoppedByMaxTime {
		t.Fatalf("Stats = %+v, want stoppedBy maxTime, not reachedEnd", stats)
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("Run() took %s, want it to return near the 20ms maxTime bound, not the full 200ms latency", elapsed)
	}
	// The injected clock never advances, so ElapsedMs comes from it, not
	// from real wall time (TECH-SPEC §4.9).
	if stats.ElapsedMs != 0 {
		t.Errorf("ElapsedMs = %d, want 0 (fixed injected clock)", stats.ElapsedMs)
	}
}

func TestScanRun_EndSnapshot_IgnoresRecordsAppendedMidScan(t *testing.T) {
	t.Parallel()
	f := fake.New()
	// The model holds 4 records, but the end snapshot was captured when
	// only the first 2 existed — Run must stop there regardless (FUNC-SPEC
	// §9.2: "a hot topic cannot extend the scan").
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
		kafka.Record{Partition: 0, Value: []byte("d")},
	)
	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 2}}}
	emit, items := collector()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(*items) != 2 || !stats.ReachedEnd || stats.StoppedBy != "" {
		t.Fatalf("Stats = %+v, items = %d, want exactly 2 items, reachedEnd, no stoppedBy", stats, len(*items))
	}
	if stats.Continuation[0] != 2 {
		t.Errorf("Continuation[0] = %d, want 2 (never advanced past End)", stats.Continuation[0])
	}
}

func TestScanRun_Continuation_NoGapNoOverlapAcrossPages(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
		kafka.Record{Partition: 0, Value: []byte("d")},
	)

	spec1 := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 4}}, MaxMessages: 2}
	emit1, page1 := collector()
	stats1, err := scan.Run(context.Background(), f, spec1, scan.MatchAll, emit1, fixedNow)
	if err != nil {
		t.Fatalf("page 1 Run() error: %v", err)
	}

	spec2 := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: stats1.Continuation[0], End: 4}}}
	emit2, page2 := collector()
	if _, err := scan.Run(context.Background(), f, spec2, scan.MatchAll, emit2, fixedNow); err != nil {
		t.Fatalf("page 2 Run() error: %v", err)
	}

	if len(*page1) != 2 || len(*page2) != 2 {
		t.Fatalf("page1 = %d items, page2 = %d items, want 2 and 2", len(*page1), len(*page2))
	}
	got := []string{(*page1)[0].Value, (*page1)[1].Value, (*page2)[0].Value, (*page2)[1].Value}
	want := []string{"a", "b", "c", "d"}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("no gap/overlap violated: got %v, want %v", got, want)
		}
	}
}

func TestScanRun_Latest_StartsAtEndMinusLimitSortedDescTruncated(t *testing.T) {
	t.Parallel()
	f := fake.New()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("0"), Timestamp: base},
		kafka.Record{Partition: 0, Value: []byte("1"), Timestamp: base.Add(1 * time.Second)},
		kafka.Record{Partition: 0, Value: []byte("2"), Timestamp: base.Add(2 * time.Second)},
		kafka.Record{Partition: 0, Value: []byte("3"), Timestamp: base.Add(3 * time.Second)},
		kafka.Record{Partition: 0, Value: []byte("4"), Timestamp: base.Add(4 * time.Second)},
	)
	// Caller-resolved: begin=0, end=5, limit=3 -> start = max(0, 5-3) = 2.
	spec := scan.Spec{
		Topic:       "t",
		Partitions:  map[int32]scan.PartitionSpec{0: {Start: 2, End: 5}},
		Latest:      true,
		MaxMessages: 3,
	}
	emit, items := collector()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(*items) != 3 {
		t.Fatalf("items = %d, want 3", len(*items))
	}
	want := []string{"4", "3", "2"}
	for i, w := range want {
		if (*items)[i].Value != w {
			t.Errorf("items[%d].Value = %q, want %q (descending timestamp order)", i, (*items)[i].Value, w)
		}
	}
	if stats.Matched != 3 {
		t.Errorf("Matched = %d, want 3", stats.Matched)
	}
}

func TestScanRun_Latest_ContinuationIsStartOffsetsReachedEndTrue(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
	)
	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 1, End: 2}}, Latest: true, MaxMessages: 1}
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, func(scan.Record) {}, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if !stats.ReachedEnd || stats.StoppedBy != "" {
		t.Fatalf("Stats = %+v, want reachedEnd=true, stoppedBy=\"\"", stats)
	}
	if stats.Continuation[0] != 1 {
		t.Errorf("Continuation[0] = %d, want 1 (the resolved start offset used, not the next-unread offset)", stats.Continuation[0])
	}
}

func TestScanRun_To_ClampsToEndSnapshot(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	// The caller resolved to=offset 2 as min(to, endSnapshot) — the true end
	// snapshot is 3, but the request's `to` caps it lower (FUNC-SPEC §9.1
	// rules C9).
	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 2}}}
	emit, items := collector()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, emit, fixedNow)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if len(*items) != 2 || !stats.ReachedEnd || stats.StoppedBy != "" {
		t.Fatalf("Stats = %+v, items = %d, want exactly 2 items clamped at to=2, reachedEnd, no stoppedBy", stats, len(*items))
	}
}

func TestScanRun_MaxTimeIsWallClockFromResolve(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	// Latency lands on Poll, not Assign, yet still eats into the same
	// budget: the deadline is set once, at Resolve, before Assign — not
	// reset per call (FUNC-SPEC §9.2: "maxTime is wall-clock from Resolve").
	f.Latency("Poll", 200*time.Millisecond)

	spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: 1}}, MaxTime: 30 * time.Millisecond}
	start := time.Now()
	stats, err := scan.Run(context.Background(), f, spec, scan.MatchAll, func(scan.Record) {}, fixedNow)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if stats.StoppedBy != scan.StoppedByMaxTime {
		t.Fatalf("StoppedBy = %q, want maxTime", stats.StoppedBy)
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("Run() took %s, want it bounded near maxTime (30ms), not the full 200ms latency", elapsed)
	}
}
