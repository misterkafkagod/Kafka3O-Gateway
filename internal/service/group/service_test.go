package group_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/group"
)

func TestGroupService_List_StateFilterAndStablePaging(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedGroup("g-charlie", nil)
	f.SeedGroupMeta("g-charlie", "Stable", "consumer", 1)
	f.SeedGroup("g-alpha", nil)
	f.SeedGroupMeta("g-alpha", "Stable", "consumer", 1)
	f.SeedGroup("g-bravo", nil)
	f.SeedGroupMeta("g-bravo", "Empty", "consumer", 1)
	svc := group.New(f)

	// State filter: only the two Stable groups.
	stable, page, err := svc.List(context.Background(), group.ListParams{State: "Stable", Page: 1, PageSize: 50})
	if err != nil {
		t.Fatalf("List(state=Stable) error: %v", err)
	}
	if page.Total != 2 || len(stable) != 2 {
		t.Fatalf("List(state=Stable) = %+v (page %+v), want 2 items", stable, page)
	}

	// Stable paging: sorted by id, one per page.
	all, page1, err := svc.List(context.Background(), group.ListParams{Page: 1, PageSize: 1})
	if err != nil {
		t.Fatalf("List(page=1) error: %v", err)
	}
	if page1.Total != 3 || len(all) != 1 || all[0].ID != "g-alpha" {
		t.Fatalf("List(page=1, pageSize=1) = %+v (page %+v), want [g-alpha] of 3", all, page1)
	}
	page2, _, err := svc.List(context.Background(), group.ListParams{Page: 2, PageSize: 1})
	if err != nil {
		t.Fatalf("List(page=2) error: %v", err)
	}
	if len(page2) != 1 || page2[0].ID != "g-bravo" {
		t.Fatalf("List(page=2, pageSize=1) = %+v, want [g-bravo]", page2)
	}

	// Page past the end -> empty items, correct total (FUNC-SPEC §9.7 Pagination).
	empty, emptyPage, err := svc.List(context.Background(), group.ListParams{Page: 10, PageSize: 50})
	if err != nil {
		t.Fatalf("List(page=10) error: %v", err)
	}
	if len(empty) != 0 || emptyPage.Total != 3 {
		t.Fatalf("List(page past end) = %+v (page %+v), want empty items, total 3", empty, emptyPage)
	}
}

func TestGroupService_Describe_LagIsEndMinusCommittedAndTotal(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("orders", 2,
		kafka.Record{Partition: 0, Value: []byte("a")},
		kafka.Record{Partition: 0, Value: []byte("b")},
		kafka.Record{Partition: 0, Value: []byte("c")},
		kafka.Record{Partition: 1, Value: []byte("d")},
		kafka.Record{Partition: 1, Value: []byte("e")},
	)
	f.SeedGroup("g1", map[kafka.TopicPartition]int64{
		{Topic: "orders", Partition: 0}: 1, // end 3 -> lag 2
		{Topic: "orders", Partition: 1}: 2, // end 2 -> lag 0
	})
	f.SeedGroupMeta("g1", "Stable", "consumer", 1)
	svc := group.New(f)

	d, err := svc.Describe(context.Background(), "g1")
	if err != nil {
		t.Fatalf("Describe() error: %v", err)
	}
	if d.ID != "g1" || d.State != "Stable" {
		t.Errorf("Describe() = %+v, want ID g1 State Stable", d)
	}
	if len(d.Offsets) != 2 {
		t.Fatalf("Offsets = %+v, want 2 entries", d.Offsets)
	}
	byPartition := map[int32]group.OffsetDetail{}
	for _, o := range d.Offsets {
		byPartition[o.Partition] = o
	}
	if got := byPartition[0]; got.Committed != 1 || got.End != 3 || got.Lag != 2 {
		t.Errorf("partition 0 = %+v, want {Committed:1 End:3 Lag:2}", got)
	}
	if got := byPartition[1]; got.Committed != 2 || got.End != 2 || got.Lag != 0 {
		t.Errorf("partition 1 = %+v, want {Committed:2 End:2 Lag:0}", got)
	}
	if d.TotalLag != 2 {
		t.Errorf("TotalLag = %d, want 2 (sum of per-partition lag)", d.TotalLag)
	}
}

func TestGroupService_Describe_MissingGroupNotFound(t *testing.T) {
	t.Parallel()
	f := fake.New()
	svc := group.New(f)

	_, err := svc.Describe(context.Background(), "does-not-exist")
	if !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("Describe(missing) error = %v, want *kafka.Error{Kind: KindNotFound}", err)
	}
}

func TestGroupService_ConsumersOfTopic_ReverseLookup(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("orders", 1, kafka.Record{Value: []byte("a")}, kafka.Record{Value: []byte("b")})
	f.SeedTopic("payments", 1, kafka.Record{Value: []byte("c")})

	// g1 consumes orders; g2 consumes payments; neither consumes the other's topic.
	f.SeedGroup("g1", map[kafka.TopicPartition]int64{{Topic: "orders", Partition: 0}: 1})
	f.SeedGroupMeta("g1", "Empty", "consumer", 1) // stopped, but still has committed offsets
	f.SeedGroup("g2", map[kafka.TopicPartition]int64{{Topic: "payments", Partition: 0}: 1})
	f.SeedGroupMeta("g2", "Stable", "consumer", 1)
	svc := group.New(f)

	groups, err := svc.ConsumersOfTopic(context.Background(), "orders")
	if err != nil {
		t.Fatalf("ConsumersOfTopic() error: %v", err)
	}
	if len(groups) != 1 || groups[0].GroupID != "g1" {
		t.Fatalf("ConsumersOfTopic(orders) = %+v, want only g1", groups)
	}
	if groups[0].TotalLag != 1 || len(groups[0].Partitions) != 1 {
		t.Errorf("g1 = %+v, want TotalLag 1, one partition", groups[0])
	}
}

func TestGroupService_ConsumersOfTopic_MissingTopicNotFound(t *testing.T) {
	t.Parallel()
	f := fake.New()
	svc := group.New(f)

	_, err := svc.ConsumersOfTopic(context.Background(), "does-not-exist")
	if !kafka.IsKind(err, kafka.KindNotFound) {
		t.Fatalf("ConsumersOfTopic(missing topic) error = %v, want *kafka.Error{Kind: KindNotFound}", err)
	}
}
