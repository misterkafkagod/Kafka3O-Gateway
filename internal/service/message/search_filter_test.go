package message_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

func TestMessageService_Search_MaxMatchesStopsBeforeMaxScan(t *testing.T) {
	t.Parallel()
	f := fake.New()
	records := make([]kafka.Record, 8)
	for i := range records {
		records[i] = kafka.Record{Partition: 0, Value: []byte("MATCH")}
	}
	f.SeedTopic("t", 1, records...)
	svc := message.New(f, consumerFactory(f), testBounds())

	// testBounds: MaxMatches default 2, MaxScan default 10. Every record
	// matches, so 2 matches arrive well before 10 records are scanned.
	result, err := svc.Search(context.Background(), message.SearchParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning}, Regex: "MATCH",
	})
	if err != nil {
		t.Fatalf("Search() error: %v", err)
	}
	if result.Stats.Matched != 2 || result.Stats.Scanned != 2 {
		t.Fatalf("Stats = %+v, want Matched=2 Scanned=2 (maxMatches stopped the scan first)", result.Stats)
	}
	if result.Stats.StoppedBy != scan.StoppedByMaxMessages {
		t.Errorf("StoppedBy = %q, want %q", result.Stats.StoppedBy, scan.StoppedByMaxMessages)
	}
}

func TestMessageService_Search_MaxScanStopsBeforeMaxMatches(t *testing.T) {
	t.Parallel()
	f := fake.New()
	records := make([]kafka.Record, 15)
	for i := range records {
		records[i] = kafka.Record{Partition: 0, Value: []byte("nope")}
	}
	f.SeedTopic("t", 1, records...)
	svc := message.New(f, consumerFactory(f), testBounds())

	// testBounds: MaxScan default 10, MaxMatches default 2. Nothing ever
	// matches, so the scan is bounded by MaxScan before MaxMatches could
	// ever be reached.
	result, err := svc.Search(context.Background(), message.SearchParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning}, Regex: "MATCH",
	})
	if err != nil {
		t.Fatalf("Search() error: %v", err)
	}
	if result.Stats.Scanned != 10 || result.Stats.Matched != 0 {
		t.Fatalf("Stats = %+v, want Scanned=10 Matched=0 (maxScan stopped the scan first)", result.Stats)
	}
	if result.Stats.ReachedEnd {
		t.Error("ReachedEnd = true, want false (stopped by maxScan, not exhaustion)")
	}
	if result.Stats.StoppedBy != scan.StoppedByMaxMessages {
		t.Errorf("StoppedBy = %q, want %q", result.Stats.StoppedBy, scan.StoppedByMaxMessages)
	}
}

func TestMessageService_Search_MaxMatchesAboveCeilingIsBoundExceeded(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t", 1, kafka.Record{Partition: 0, Value: []byte("a")})
	svc := message.New(f, consumerFactory(f), testBounds())

	_, err := svc.Search(context.Background(), message.SearchParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning}, Regex: "a", MaxMatches: 999,
	})
	if !core.IsCode(err, core.BoundExceeded) {
		t.Fatalf("Search(maxMatches above ceiling) error = %v, want *core.PolicyError{Code: BoundExceeded}", err)
	}
}

func TestMessageService_Filter_SkippedCountedNotErrored(t *testing.T) {
	t.Parallel()
	f := fake.New()
	records := []kafka.Record{
		{Partition: 0, Value: []byte(`{"status":"FAILED"}`)},
		{Partition: 0, Value: []byte("not json at all")},
		{Partition: 0, Value: []byte(`{"status":"OK"}`)},
	}
	f.SeedTopic("t", 1, records...)
	svc := message.New(f, consumerFactory(f), testBounds())

	result, err := svc.Filter(context.Background(), message.FilterParams{
		Topic: "t", From: message.From{Kind: message.FromBeginning},
		Path: "$.status", Op: scan.OpEq, Value: "FAILED",
	})
	if err != nil {
		t.Fatalf("Filter() error: %v", err)
	}
	if result.Stats.Skipped != 1 {
		t.Errorf("Stats.Skipped = %d, want 1 (Stats=%+v)", result.Stats.Skipped, result.Stats)
	}
	if len(result.Items) != 1 {
		t.Errorf("Items = %d, want 1 (only the FAILED record matches)", len(result.Items))
	}
}
