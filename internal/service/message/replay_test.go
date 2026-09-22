package message_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

func replayCaller() core.Caller { return core.Caller{Tier: core.TierOperator} }

func TestReplay_Plan_ResolvesPartitionsSnapshotAndEstimate(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
		kafka.Record{Partition: 0, Value: []byte("d")},
		kafka.Record{Partition: 0, Value: []byte("e")},
	)
	f.SeedTopic("dst", 1)
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	plan, err := svc.PlanReplay(context.Background(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 0)
	if err != nil {
		t.Fatalf("PlanReplay() error: %v", err)
	}
	if plan.EstimatedRecords != 5 || plan.SourcePartitions != 1 || plan.TargetPartitions != 1 {
		t.Fatalf("plan = %+v, want EstimatedRecords 5, SourcePartitions 1, TargetPartitions 1", plan)
	}
	if plan.ConfirmTarget() != "dst" {
		t.Errorf("ConfirmTarget() = %q, want dst", plan.ConfirmTarget())
	}
}

func TestReplay_Plan_PreservePartitionMismatch(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 1, Value: []byte("b")},
	)
	f.SeedTopic("dst", 1)
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	_, err := svc.PlanReplay(context.Background(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst", PreservePartition: true}, 0)
	if !core.IsCode(err, core.PartitionMismatch) {
		t.Fatalf("PlanReplay(preservePartition, fewer target partitions) error = %v, want *core.PolicyError{Code: PartitionMismatch}", err)
	}
}

func TestReplay_Apply_CopiedCountAndCursor(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	f.SeedTopic("dst", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	result, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 2, "dst", false)
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if result.Value.Copied != 2 || result.Value.Cursor[0] != 2 || result.Value.ReachedEnd {
		t.Fatalf("Replay() = %+v, want Copied 2, Cursor[0] 2, ReachedEnd false", result.Value)
	}
}

func TestReplay_Apply_ResumeFromCursorNoGapNoOverlap(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
		kafka.Record{Partition: 0, Value: []byte("d")},
		kafka.Record{Partition: 0, Value: []byte("e")},
	)
	f.SeedTopic("dst", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	first, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 2, "dst", false)
	if err != nil {
		t.Fatalf("Replay(first) error: %v", err)
	}
	if first.Value.Copied != 2 || first.Value.ReachedEnd {
		t.Fatalf("Replay(first) = %+v, want Copied 2, ReachedEnd false", first.Value)
	}

	// limit is comfortably above the 3 remaining records, not exactly 3: when
	// the message limit and the window's own end coincide on the same
	// record, scan.Run reports StoppedByMaxMessages rather than exhausted
	// (the bound is checked before the next windowsExhausted check), so an
	// exact-fit limit would not exercise ReachedEnd here.
	second, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromOffset, Offset: first.Value.Cursor[0]}},
		message.ReplayTarget{Topic: "dst"}, 10, "dst", false)
	if err != nil {
		t.Fatalf("Replay(second) error: %v", err)
	}
	if second.Value.Copied != 3 || !second.Value.ReachedEnd {
		t.Fatalf("Replay(second) = %+v, want Copied 3, ReachedEnd true", second.Value)
	}

	end, err := f.ListEndOffsets(context.Background(), "dst")
	if err != nil {
		t.Fatalf("ListEndOffsets() error: %v", err)
	}
	if end[0] != 5 {
		t.Errorf("dst end offset = %d, want 5 (no gap, no overlap across the two calls)", end[0])
	}
}

func TestReplay_Apply_ReachedEndOnLastBatch(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	f.SeedTopic("dst", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	result, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 10, "dst", false)
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}
	if result.Value.Copied != 3 || !result.Value.ReachedEnd {
		t.Fatalf("Replay() = %+v, want Copied 3, ReachedEnd true", result.Value)
	}
}

// TestReplay_Apply_MidBatchProduceFailureIsKafkaErrorWithProgress proves the
// 502 KAFKA_ERROR / details.progress path a produce failure reports
// (FUNC-SPEC §9.3 step 4): the first call fully copies part of the range
// (proving copied > 0 and a correct cursor from a real batch produce), and
// the resumed call — with FailNext armed on the fake's Produce — fails
// immediately, reporting the progress a caller retries from. The fake's
// three spec'd fault primitives (TECH-SPEC §4.3) fail a whole Produce call,
// not one record inside a batch, so this is the closest deterministic
// reproduction of "a produce failure mid-batch" without depending on the
// fake's map-iteration-ordered cross-partition record ordering.
func TestReplay_Apply_MidBatchProduceFailureIsKafkaErrorWithProgress(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	f.SeedTopic("dst", 1)
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	first, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 2, "dst", false)
	if err != nil {
		t.Fatalf("Replay(first) error: %v", err)
	}
	if first.Value.Copied != 2 {
		t.Fatalf("Replay(first) = %+v, want Copied 2", first.Value)
	}

	f.FailNext("Produce", kafka.KindBroker)
	_, err = svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromOffset, Offset: first.Value.Cursor[0]}},
		message.ReplayTarget{Topic: "dst"}, 1, "dst", false)
	if !core.IsCode(err, core.ReplayFailed) {
		t.Fatalf("Replay(second, produce fails) error = %v, want *core.PolicyError{Code: ReplayFailed}", err)
	}
	var pe *core.PolicyError
	if errors.As(err, &pe) {
		progress, _ := pe.Details["progress"].(map[string]any)
		if progress["copied"] != 0 {
			t.Errorf("details.progress.copied = %v, want 0", progress["copied"])
		}
		cursor, _ := progress["cursor"].(map[int32]int64)
		if cursor[0] != first.Value.Cursor[0] {
			t.Errorf("details.progress.cursor = %v, want {0: %d} (unchanged: this call copied nothing)", progress["cursor"], first.Value.Cursor[0])
		}
	}

	events := rec.Events()
	if len(events) != 4 {
		t.Fatalf("Events() = %+v, want 4 (ATTEMPT+RESULT per call)", events)
	}
	if events[3].Phase != audit.PhaseResult || events[3].Outcome != audit.OutcomeFailed {
		t.Fatalf("second call's RESULT = %+v, want Phase RESULT, Outcome FAILED", events[3])
	}
}

func TestReplay_Apply_HeadersTimestampsKeyByteEqual(t *testing.T) {
	t.Parallel()
	f := fake.New()
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	f.SeedTopic("src", 1, kafka.Record{
		Partition: 0, Key: []byte{0xff, 0x00, 0x01}, Value: []byte(`{"a":1}`),
		Headers:   []kafka.Header{{Key: "h1", Value: []byte{0xde, 0xad, 0xbe, 0xef}}},
		Timestamp: ts,
	})
	f.SeedTopic("dst", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	_, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 10, "dst", false)
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}

	if err := f.Assign(context.Background(), "dst", []int32{0}, map[int32]int64{0: 0}); err != nil {
		t.Fatalf("Assign() error: %v", err)
	}
	defer f.Close()
	polled, err := f.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll() error: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("Poll() = %d records, want 1", len(polled))
	}
	r := polled[0]
	if string(r.Key) != string([]byte{0xff, 0x00, 0x01}) {
		t.Errorf("key = %v, want ff 00 01", r.Key)
	}
	if string(r.Value) != `{"a":1}` {
		t.Errorf("value = %q, want {\"a\":1}", r.Value)
	}
	if len(r.Headers) != 1 || string(r.Headers[0].Value) != string([]byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("headers = %+v, want one header de ad be ef", r.Headers)
	}
	if !r.Timestamp.Equal(ts) {
		t.Errorf("timestamp = %v, want %v", r.Timestamp, ts)
	}
}

func TestReplay_LimitAboveCeilingIsBoundExceeded(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1)
	f.SeedTopic("dst", 1)
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	_, err := svc.PlanReplay(context.Background(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 999999)
	if !core.IsCode(err, core.BoundExceeded) {
		t.Fatalf("PlanReplay(limit above ceiling) error = %v, want *core.PolicyError{Code: BoundExceeded}", err)
	}
}

func TestReplay_ConfirmIsTargetTopic(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	f.SeedTopic("dst", 1)
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	_, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 0, "src", false)
	if !core.IsCode(err, core.ConfirmationMismatch) {
		t.Fatalf("Replay(confirm=source) error = %v, want *core.PolicyError{Code: ConfirmationMismatch}", err)
	}
	if len(rec.Events()) != 0 {
		t.Errorf("Events() = %+v, want none (a confirm mismatch reports nothing)", rec.Events())
	}
}

func TestReplay_UsesScanRunAndAttemptResultAudit(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("src", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	f.SeedTopic("dst", 1)
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBounds(), auditor, testRunner())

	_, err := svc.Replay(context.Background(), replayCaller(),
		message.ReplaySource{Topic: "src", From: message.From{Kind: message.FromBeginning}},
		message.ReplayTarget{Topic: "dst"}, 0, "dst", false)
	if err != nil {
		t.Fatalf("Replay() error: %v", err)
	}

	events := rec.Events()
	if len(events) != 2 || events[0].Phase != audit.PhaseAttempt || events[1].Phase != audit.PhaseResult {
		t.Fatalf("Events() = %+v, want [ATTEMPT, RESULT]", events)
	}
	if events[1].Outcome != audit.OutcomeSucceeded {
		t.Errorf("RESULT.Outcome = %s, want SUCCEEDED", events[1].Outcome)
	}
	f.AssertCalled(t, "Poll")
}
