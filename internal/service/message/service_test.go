package message_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

func consumerFactory(f *fake.Fake) message.ConsumerFactory {
	return func() (kafka.Consumer, error) { return f, nil }
}

func testBounds() message.Bounds {
	return message.Bounds{
		Limit:      message.Range{Default: 2, Ceiling: 5},
		MaxScan:    message.Range{Default: 10, Ceiling: 20},
		MaxMatches: message.Range{Default: 2, Ceiling: 5},
		MaxBytes:   message.RangeBytes{Default: 1 << 20, Ceiling: 1 << 21},
		MaxTime:    message.RangeDuration{Default: 0, Ceiling: 0}, // unbounded in tests: 0 = no context timeout
		Replay:     message.Range{Default: 1000, Ceiling: 10000},
	}
}

func TestMessageService_Read_LimitAboveCeilingIsBoundExceeded(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	_, err := svc.Read(context.Background(), core.Caller{}, message.ReadParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning}, Limit: 999,
	})
	if !core.IsCode(err, core.BoundExceeded) {
		t.Fatalf("Read(limit above ceiling) error = %v, want *core.PolicyError{Code: BoundExceeded}", err)
	}
}

func TestMessageService_Read_DefaultsApplied(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	// Limit left zero -> Bounds.Limit.Default (2) applies, stopping before
	// the third seeded record.
	result, err := svc.Read(context.Background(), core.Caller{}, message.ReadParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning},
	})
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("Items = %d, want 2 (the default limit)", len(result.Items))
	}
	if result.Stats.StoppedBy != "maxMessages" {
		t.Errorf("StoppedBy = %q, want maxMessages", result.Stats.StoppedBy)
	}
}

func TestMessageService_Read_NoCommitsNoGroupJoin(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	if _, err := svc.Read(context.Background(), core.Caller{}, message.ReadParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning},
	}); err != nil {
		t.Fatalf("Read() error: %v", err)
	}

	f.AssertNoCommits(t)
	f.AssertNoGroupJoin(t)
}

func TestMessageService_Get_ReturnsRecordAtOffset(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	r, err := svc.Get(context.Background(), core.Caller{}, "t", 0, 1)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if r.Offset != 1 || r.Value != "b" {
		t.Fatalf("Get(offset=1) = %+v, want offset 1 value b", r)
	}
}

func TestMessageService_Get_CompactedAwayIsNotFound(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
	)
	// Offsets 0 and 1 are compacted away; the log now starts at 2.
	f.SeedCompactAway("t", 0, 2)
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	_, err := svc.Get(context.Background(), core.Caller{}, "t", 0, 0)
	if !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("Get(compacted-away offset) error = %v, want *kafka.Error{Kind: KindNotFound}", err)
	}
}

func TestMessageService_Get_OutOfRangeIsNotFound(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	svc := message.New(f, f, consumerFactory(f), testBounds(), nil, testRunner())

	_, err := svc.Get(context.Background(), core.Caller{}, "t", 0, 999999)
	if !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("Get(out-of-range offset) error = %v, want *kafka.Error{Kind: KindNotFound}", err)
	}
}
