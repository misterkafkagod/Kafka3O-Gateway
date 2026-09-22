package message_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/audit/audittest"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/gates"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

func newTestAuditor() (*audit.Auditor, *audittest.RecordingSink) {
	rec := audittest.New()
	return audit.NewAuditor(rec), rec
}

func testRunner() core.Runner {
	return core.Runner{Check: gates.Check}
}

func testBoundsWithBulkBody(limit int64) message.Bounds {
	b := testBounds()
	b.MaxBulkBodyBytes = limit
	return b
}

func TestMessageService_Produce_SingleObjectAndArrayAccepted(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBoundsWithBulkBody(1<<20), auditor, testRunner())

	// M5 accepts a single record object or a records[] array (FUNC-SPEC §8.7
	// M5); that JSON-shape normalisation happens in the API DTO (Task 5.5) —
	// at the service layer, both already arrive as a []ProduceItem, so a
	// 1-item slice stands in for "single object" here.
	single, err := svc.Produce(context.Background(), core.Caller{Tier: core.TierOperator}, "t", []message.ProduceItem{
		{Value: "hello"},
	})
	if err != nil {
		t.Fatalf("Produce(single) error: %v", err)
	}
	if len(single.Items) != 1 || single.Items[0].Outcome != audit.OutcomeSucceeded {
		t.Fatalf("Produce(single) = %+v, want one SUCCEEDED item", single)
	}

	array, err := svc.Produce(context.Background(), core.Caller{Tier: core.TierOperator}, "t", []message.ProduceItem{
		{Value: "a"}, {Value: "b"}, {Value: "c"},
	})
	if err != nil {
		t.Fatalf("Produce(array) error: %v", err)
	}
	if array.Summary != (core.BulkSummary{Total: 3, Succeeded: 3, Failed: 0}) {
		t.Errorf("Produce(array).Summary = %+v, want {Total:3 Succeeded:3 Failed:0}", array.Summary)
	}
}

func TestMessageService_Produce_PartitionOutOfRangeInvalid(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 2)
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBoundsWithBulkBody(1<<20), auditor, testRunner())

	outOfRange := int32(99)
	_, err := svc.Produce(context.Background(), core.Caller{Tier: core.TierOperator}, "t", []message.ProduceItem{
		{Value: "a", Partition: &outOfRange},
	})

	if !core.IsCode(err, core.BulkValidationFailed) {
		t.Fatalf("Produce(out-of-range partition) error = %v, want *core.PolicyError{Code: BulkValidationFailed}", err)
	}
	if events := rec.Events(); len(events) != 0 {
		t.Errorf("Events() = %+v, want none (no ATTEMPT on validation failure)", events)
	}
	if len(f.MutatingCalls()) != 0 {
		t.Errorf("MutatingCalls() = %d, want 0", len(f.MutatingCalls()))
	}
}

func TestMessageService_Produce_EncodingsApplied(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBoundsWithBulkBody(1<<20), auditor, testRunner())

	wantValue := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	result, err := svc.Produce(context.Background(), core.Caller{Tier: core.TierOperator}, "t", []message.ProduceItem{
		{
			Key: "the-key", KeyEncoding: "string",
			Value: base64.StdEncoding.EncodeToString(wantValue), ValueEncoding: "base64",
		},
	})
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}
	if result.Items[0].Outcome != audit.OutcomeSucceeded {
		t.Fatalf("Produce() item = %+v, want SUCCEEDED", result.Items[0])
	}

	got, err := svc.Get(context.Background(), core.Caller{}, "t", result.Items[0].Partition, result.Items[0].Offset)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.Key != "the-key" || got.ValueEncoding != "base64" {
		t.Fatalf("Get() = %+v, want key %q base64-encoded value", got, "the-key")
	}
	decoded, err := base64.StdEncoding.DecodeString(got.Value)
	if err != nil || !bytes.Equal(decoded, wantValue) {
		t.Fatalf("Get().Value decodes to %x (err %v), want %x", decoded, err, wantValue)
	}
}

func TestMessageService_ProduceBulk_NDJSONAndJSONArray(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1)
	auditor, _ := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBoundsWithBulkBody(1<<20), auditor, testRunner())

	ndjson := strings.NewReader("{\"value\":\"a\"}\n{\"value\":\"b\"}\n")
	ndjsonResult, err := svc.ProduceBulk(context.Background(), core.Caller{Tier: core.TierOperator}, "t", ndjson, true)
	if err != nil {
		t.Fatalf("ProduceBulk(ndjson) error: %v", err)
	}
	if ndjsonResult.Summary != (core.BulkSummary{Total: 2, Succeeded: 2, Failed: 0}) {
		t.Errorf("ProduceBulk(ndjson).Summary = %+v, want {Total:2 Succeeded:2 Failed:0}", ndjsonResult.Summary)
	}

	jsonArray := strings.NewReader(`[{"value":"c"},{"value":"d"},{"value":"e"}]`)
	arrayResult, err := svc.ProduceBulk(context.Background(), core.Caller{Tier: core.TierOperator}, "t", jsonArray, false)
	if err != nil {
		t.Fatalf("ProduceBulk(array) error: %v", err)
	}
	if arrayResult.Summary != (core.BulkSummary{Total: 3, Succeeded: 3, Failed: 0}) {
		t.Errorf("ProduceBulk(array).Summary = %+v, want {Total:3 Succeeded:3 Failed:0}", arrayResult.Summary)
	}
}

func TestMessageService_ProduceBulk_BodyLimitIs413(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1)
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBoundsWithBulkBody(16), auditor, testRunner())

	oversized := strings.NewReader(`[{"value":"this body is well over sixteen bytes"}]`)
	_, err := svc.ProduceBulk(context.Background(), core.Caller{Tier: core.TierOperator}, "t", oversized, false)

	if !core.IsCode(err, core.PayloadTooLarge) {
		t.Fatalf("ProduceBulk(oversized) error = %v, want *core.PolicyError{Code: PayloadTooLarge}", err)
	}
	if events := rec.Events(); len(events) != 0 {
		t.Errorf("Events() = %+v, want none (no ATTEMPT on oversized body)", events)
	}
	if len(f.MutatingCalls()) != 0 {
		t.Errorf("MutatingCalls() = %d, want 0", len(f.MutatingCalls()))
	}
}

func TestMessageService_Tombstone_NullValueProduced(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1)
	auditor, rec := newTestAuditor()
	svc := message.New(f, f, consumerFactory(f), testBoundsWithBulkBody(1<<20), auditor, testRunner())

	result, err := svc.Tombstone(context.Background(), core.Caller{Tier: core.TierOperator}, "t", message.TombstoneItem{Key: "k1"})
	if err != nil {
		t.Fatalf("Tombstone() error: %v", err)
	}

	// Read the raw kafka.Record back (rather than through Get, which decodes
	// via internal/scan.Decode — Record.Value there is a string with no null
	// concept, so a nil value and an empty one decode identically): the port
	// itself, where kafka.Record.Value is []byte, is where "the value is
	// nil" is actually observable.
	if err := f.Assign(context.Background(), "t", []int32{result.Partition}, map[int32]int64{result.Partition: result.Offset}); err != nil {
		t.Fatalf("Assign() error: %v", err)
	}
	polled, err := f.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll() error: %v", err)
	}
	if len(polled) != 1 {
		t.Fatalf("Poll() = %d records, want 1", len(polled))
	}
	if polled[0].Value != nil {
		t.Errorf("produced record Value = %q, want nil (tombstone)", polled[0].Value)
	}
	if string(polled[0].Key) != "k1" {
		t.Errorf("produced record Key = %q, want %q", polled[0].Key, "k1")
	}

	events := rec.Events()
	if len(events) != 2 || events[0].Phase != audit.PhaseAttempt || events[1].Phase != audit.PhaseResult {
		t.Fatalf("Events() = %+v, want [ATTEMPT, RESULT]", events)
	}
	if events[1].Outcome != audit.OutcomeSucceeded {
		t.Errorf("RESULT.Outcome = %s, want SUCCEEDED", events[1].Outcome)
	}
}
