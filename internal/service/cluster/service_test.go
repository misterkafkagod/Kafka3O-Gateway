package cluster_test

import (
	"context"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/service/cluster"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestClusterService_DescribeCluster_MapsBrokersAndController(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedBroker(1, "b1", 9092, "rack-a")
	f.SeedBroker(2, "b2", 9092, "")
	svc := cluster.New(f, nil, core.Runner{})

	info, err := svc.DescribeCluster(context.Background())
	if err != nil {
		t.Fatalf("DescribeCluster() error: %v", err)
	}
	if info.ControllerID != 1 {
		t.Errorf("ControllerID = %d, want 1 (first seeded broker)", info.ControllerID)
	}
	found := map[int32]bool{}
	for _, b := range info.Brokers {
		found[b.ID] = true
	}
	if len(info.Brokers) != 2 || !found[1] || !found[2] {
		t.Errorf("Brokers = %+v, want ids 1 and 2", info.Brokers)
	}
}

func TestClusterService_DescribeBrokerConfig_SensitiveValueNull(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedBroker(1, "b1", 9092, "")
	f.SeedBrokerConfigs(1,
		kafka.ConfigEntry{Name: "log.retention.ms", Value: "604800000"},
		kafka.ConfigEntry{Name: "sasl.jaas.config", Value: "super-secret", IsSensitive: true},
	)
	svc := cluster.New(f, nil, core.Runner{})

	configs, err := svc.DescribeBrokerConfig(context.Background(), 1)
	if err != nil {
		t.Fatalf("DescribeBrokerConfig() error: %v", err)
	}
	var sawSensitive bool
	for _, c := range configs {
		if c.Name != "sasl.jaas.config" {
			continue
		}
		sawSensitive = true
		if c.Value != "" {
			t.Errorf("sensitive config Value = %q, want empty", c.Value)
		}
	}
	if !sawSensitive {
		t.Fatal("DescribeBrokerConfig() did not return the seeded sensitive config")
	}
}

func TestClusterService_HealthSummary_CountsURPsOfflineNonPreferred(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedBroker(1, "b1", 9092, "")
	f.SeedBroker(2, "b2", 9092, "")

	f.SeedTopic("t-healthy", 1, kafka.Record{Value: []byte("v")})
	f.SeedPartitionMeta("t-healthy", 0, 1, []int32{1, 2}, []int32{1, 2})

	f.SeedTopic("t-under-replicated", 1, kafka.Record{Value: []byte("v")})
	f.SeedPartitionMeta("t-under-replicated", 0, 1, []int32{1, 2}, []int32{1})

	f.SeedTopic("t-offline", 1, kafka.Record{Value: []byte("v")})
	f.SeedPartitionMeta("t-offline", 0, -1, []int32{1, 2}, []int32{1, 2})

	f.SeedTopic("t-non-preferred", 1, kafka.Record{Value: []byte("v")})
	f.SeedPartitionMeta("t-non-preferred", 0, 2, []int32{1, 2}, []int32{1, 2})

	svc := cluster.New(f, nil, core.Runner{})
	summary, err := svc.HealthSummary(context.Background())
	if err != nil {
		t.Fatalf("HealthSummary() error: %v", err)
	}
	if summary.BrokersOnline != 2 {
		t.Errorf("BrokersOnline = %d, want 2", summary.BrokersOnline)
	}
	if summary.TopicsTotal != 4 {
		t.Errorf("TopicsTotal = %d, want 4", summary.TopicsTotal)
	}
	if summary.PartitionsTotal != 4 {
		t.Errorf("PartitionsTotal = %d, want 4", summary.PartitionsTotal)
	}
	if summary.PartitionsUnderReplicated != 1 {
		t.Errorf("PartitionsUnderReplicated = %d, want 1", summary.PartitionsUnderReplicated)
	}
	if summary.PartitionsOffline != 1 {
		t.Errorf("PartitionsOffline = %d, want 1", summary.PartitionsOffline)
	}
	if summary.PartitionsNonPreferredLeader != 1 {
		t.Errorf("PartitionsNonPreferredLeader = %d, want 1", summary.PartitionsNonPreferredLeader)
	}
	if len(summary.Affected) != 3 {
		t.Fatalf("Affected = %d entries, want 3 (one per issue topic)", len(summary.Affected))
	}
	if summary.Truncated {
		t.Error("Truncated = true, want false")
	}
}

func TestClusterService_HealthSummary_AffectedCappedAt1000WithTruncated(t *testing.T) {
	t.Parallel()
	f := fake.New()
	f.SeedBroker(1, "b1", 9092, "")

	const partitions = 1200
	f.SeedTopic("t-many-offline", partitions)
	for p := range partitions {
		f.SeedPartitionMeta("t-many-offline", int32(p), -1, []int32{1}, []int32{1})
	}

	svc := cluster.New(f, nil, core.Runner{})
	summary, err := svc.HealthSummary(context.Background())
	if err != nil {
		t.Fatalf("HealthSummary() error: %v", err)
	}
	if summary.PartitionsOffline != partitions {
		t.Errorf("PartitionsOffline = %d, want %d (counts stay uncapped)", summary.PartitionsOffline, partitions)
	}
	if len(summary.Affected) != 1000 {
		t.Errorf("Affected = %d entries, want 1000 (capped)", len(summary.Affected))
	}
	if !summary.Truncated {
		t.Error("Truncated = false, want true")
	}
}
