package topic_test

import (
	"context"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
	"github.com/misterkafkagod/kafka3o/internal/service/topic"
)

func seedTopics(f *fake.Fake) {
	f.SeedTopic("orders", 1, kafka.Record{Value: []byte("v")})
	f.SeedTopicMeta("orders", false, 1)
	f.SeedTopic("payments", 1, kafka.Record{Value: []byte("v")})
	f.SeedTopicMeta("payments", false, 1)
	f.SeedTopic("__consumer_offsets", 1, kafka.Record{Value: []byte("v")})
	f.SeedTopicMeta("__consumer_offsets", true, 1)
}

func TestTopicService_List_PatternIsRE2Unanchored(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedTopics(f)
	svc := topic.New(f, nil, core.Runner{})

	items, _, err := svc.List(context.Background(), topic.ListParams{Pattern: "pay", Page: 1, PageSize: 50})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(items) != 1 || items[0].Name != "payments" {
		t.Fatalf("List(pattern=pay) = %+v, want only payments (unanchored match)", items)
	}
}

func TestTopicService_List_InvalidPatternIsInvalidRegex(t *testing.T) {
	t.Parallel()
	f := fake.New()
	svc := topic.New(f, nil, core.Runner{})

	_, _, err := svc.List(context.Background(), topic.ListParams{Pattern: "(", Page: 1, PageSize: 50})
	if !core.IsCode(err, core.InvalidRegex) {
		t.Fatalf("List(invalid pattern) error = %v, want *core.PolicyError{Code: InvalidRegex}", err)
	}
}

func TestTopicService_List_SortedByNameStablePaging(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedTopics(f)
	svc := topic.New(f, nil, core.Runner{})

	items, page, err := svc.List(context.Background(), topic.ListParams{IncludeInternal: true, Page: 1, PageSize: 2})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if page.Total != 3 {
		t.Fatalf("page.Total = %d, want 3", page.Total)
	}
	if len(items) != 2 || items[0].Name != "__consumer_offsets" || items[1].Name != "orders" {
		t.Fatalf("page 1 = %+v, want [__consumer_offsets, orders] (sorted by name)", items)
	}

	items2, _, err := svc.List(context.Background(), topic.ListParams{IncludeInternal: true, Page: 2, PageSize: 2})
	if err != nil {
		t.Fatalf("List() page 2 error: %v", err)
	}
	if len(items2) != 1 || items2[0].Name != "payments" {
		t.Fatalf("page 2 = %+v, want [payments]", items2)
	}
}

func TestTopicService_List_PagePastEndEmptyWithTotal(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedTopics(f)
	svc := topic.New(f, nil, core.Runner{})

	items, page, err := svc.List(context.Background(), topic.ListParams{IncludeInternal: true, Page: 99, PageSize: 50})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("items = %+v, want empty", items)
	}
	if page.Total != 3 {
		t.Errorf("page.Total = %d, want 3", page.Total)
	}
}

func TestTopicService_List_IncludeInternalToggle(t *testing.T) {
	t.Parallel()
	f := fake.New()
	seedTopics(f)
	svc := topic.New(f, nil, core.Runner{})

	without, _, err := svc.List(context.Background(), topic.ListParams{IncludeInternal: false, Page: 1, PageSize: 50})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	for _, it := range without {
		if it.Internal {
			t.Errorf("List(includeInternal=false) returned internal topic %q", it.Name)
		}
	}
	if len(without) != 2 {
		t.Errorf("List(includeInternal=false) = %d items, want 2", len(without))
	}

	with, _, err := svc.List(context.Background(), topic.ListParams{IncludeInternal: true, Page: 1, PageSize: 50})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(with) != 3 {
		t.Errorf("List(includeInternal=true) = %d items, want 3", len(with))
	}
}

func TestTopicService_Describe_ApproxCountIsSumEndMinusBegin(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-describe", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
		kafka.Record{Partition: 1, Value: []byte("d")},
	)
	f.SeedPartitionMeta("t-describe", 0, 1, []int32{1}, []int32{1})
	f.SeedPartitionMeta("t-describe", 1, 1, []int32{1}, []int32{1})
	svc := topic.New(f, nil, core.Runner{})

	got, err := svc.Describe(context.Background(), "t-describe")
	if err != nil {
		t.Fatalf("Describe() error: %v", err)
	}
	if got.ApproxMessageCount != 4 {
		t.Errorf("ApproxMessageCount = %d, want 4 (3 + 1)", got.ApproxMessageCount)
	}
	if len(got.Partitions) != 2 || got.Partitions[0].ApproxCount != 3 || got.Partitions[1].ApproxCount != 1 {
		t.Fatalf("Partitions = %+v, want ApproxCount 3 then 1", got.Partitions)
	}
}

func TestTopicService_Describe_ConfigSourceNormalised(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-configs", 1)
	f.SeedTopicConfigs("t-configs",
		kafka.ConfigEntry{Name: "cleanup.policy", Value: "delete", Source: kafka.SourceDefault},
		kafka.ConfigEntry{Name: "retention.ms", Value: "3600000", Source: kafka.SourceStatic},
		kafka.ConfigEntry{Name: "compression.type", Value: "producer", Source: kafka.SourceDynamic},
	)
	svc := topic.New(f, nil, core.Runner{})

	got, err := svc.Describe(context.Background(), "t-configs")
	if err != nil {
		t.Fatalf("Describe() error: %v", err)
	}
	source := map[string]kafka.ConfigSource{}
	for _, c := range got.Configs {
		source[c.Name] = c.Source
	}
	want := map[string]kafka.ConfigSource{
		"cleanup.policy":   kafka.SourceDefault,
		"retention.ms":     kafka.SourceStatic,
		"compression.type": kafka.SourceDynamic,
	}
	for name, wantSource := range want {
		if source[name] != wantSource {
			t.Errorf("config %q Source = %v, want %v", name, source[name], wantSource)
		}
	}
}

func TestTopicService_Describe_MissingNotFound(t *testing.T) {
	t.Parallel()
	f := fake.New()
	svc := topic.New(f, nil, core.Runner{})

	_, err := svc.Describe(context.Background(), "does-not-exist")
	if !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("Describe(missing) error = %v, want *kafka.Error{Kind: KindNotFound}", err)
	}
}

func TestTopicService_Size_TotalsPerPartitionAndReplica(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-size", 2, kafka.Record{Partition: 0, Value: []byte("v")}, kafka.Record{Partition: 1, Value: []byte("v")})
	f.SeedLogDir("t-size", 0, 1, "/data/kafka-logs", 1000)
	f.SeedLogDir("t-size", 0, 2, "/data/kafka-logs", 1500)
	f.SeedLogDir("t-size", 1, 1, "/data/kafka-logs", 200)
	svc := topic.New(f, nil, core.Runner{})

	got, err := svc.Size(context.Background(), "t-size")
	if err != nil {
		t.Fatalf("Size() error: %v", err)
	}
	if got.TotalBytes != 2700 {
		t.Errorf("TotalBytes = %d, want 2700 (1000+1500+200)", got.TotalBytes)
	}
	if len(got.Partitions) != 2 {
		t.Fatalf("Partitions = %+v, want 2 entries", got.Partitions)
	}
	if got.Partitions[0].ID != 0 || got.Partitions[0].Bytes != 2500 || len(got.Partitions[0].Replicas) != 2 {
		t.Errorf("partition 0 = %+v, want Bytes=2500 across 2 replicas", got.Partitions[0])
	}
	if got.Partitions[1].ID != 1 || got.Partitions[1].Bytes != 200 || len(got.Partitions[1].Replicas) != 1 {
		t.Errorf("partition 1 = %+v, want Bytes=200 across 1 replica", got.Partitions[1])
	}
}

func TestTopicService_CountInWindow_ClampsToEndAndSums(t *testing.T) {
	t.Parallel()
	f := fake.New()
	base := int64(1_700_000_000_000)
	f.SeedTopic("t-window", 2,
		kafka.Record{Partition: 0, Value: []byte("a"), Timestamp: time.UnixMilli(base)},
		kafka.Record{Partition: 0, Value: []byte("b"), Timestamp: time.UnixMilli(base + 1000)},
		kafka.Record{Partition: 0, Value: []byte("c"), Timestamp: time.UnixMilli(base + 2000)},
		kafka.Record{Partition: 1, Value: []byte("d"), Timestamp: time.UnixMilli(base)},
	)
	svc := topic.New(f, nil, core.Runner{})

	// to is far beyond the last record; must clamp to the end offset rather
	// than reporting an offset that doesn't exist.
	got, err := svc.CountInWindow(context.Background(), "t-window", base, base+999_999_999)
	if err != nil {
		t.Fatalf("CountInWindow() error: %v", err)
	}
	if got.Total != 4 {
		t.Errorf("Total = %d, want 4 (all seeded records)", got.Total)
	}
	for _, p := range got.Partitions {
		if p.ID == 0 && (p.FromOffset != 0 || p.ToOffset != 3 || p.Count != 3) {
			t.Errorf("partition 0 = %+v, want from=0 to=3 count=3", p)
		}
		if p.ID == 1 && (p.FromOffset != 0 || p.ToOffset != 1 || p.Count != 1) {
			t.Errorf("partition 1 = %+v, want from=0 to=1 count=1", p)
		}
	}
}
