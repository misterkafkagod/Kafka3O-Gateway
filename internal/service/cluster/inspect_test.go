package cluster_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
)

func TestClusterService_Export_PatternAndOverridesOnly(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedTopic("t-orders", 3)
	f.SeedTopicMeta("t-orders", false, 2)
	f.SeedTopicConfigs("t-orders",
		kafka.ConfigEntry{Name: "retention.ms", Value: "60000", Source: kafka.SourceDynamic},
		kafka.ConfigEntry{Name: "segment.bytes", Value: "1073741824", Source: kafka.SourceDefault},
	)
	f.SeedTopic("other", 1)
	auditor, _ := testClusterAuditor()
	svc := cluster.New(f, auditor, testClusterRunner())

	export, err := svc.Export(context.Background(), "^t-")
	if err != nil {
		t.Fatalf("Export() error: %v", err)
	}
	if export.ExportedAt.IsZero() {
		t.Error("ExportedAt is zero, want the time of the call")
	}
	topics := export.Topics
	if len(topics) != 1 || topics[0].Name != "t-orders" {
		t.Fatalf("Export() = %+v, want exactly [t-orders]", topics)
	}
	got := topics[0]
	if got.Partitions != 3 || got.ReplicationFactor != 2 {
		t.Errorf("t-orders = %+v, want Partitions 3, ReplicationFactor 2", got)
	}
	if len(got.Configs) != 1 || got.Configs["retention.ms"] != "60000" {
		t.Errorf("t-orders.Configs = %v, want only the dynamic override retention.ms=60000", got.Configs)
	}
}
