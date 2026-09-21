package porttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// adminSeeder is the optional capability internal/kafka/fake exposes to seed
// brokers for the happy-path case. internal/kafka/franz has no equivalent —
// at Level 2 the real cluster already has whatever brokers it has — so that
// sub-test is skipped when port doesn't implement it.
type adminSeeder interface {
	SeedBroker(id int32, host string, port int32, rack string)
}

// unreachabler is the optional capability internal/kafka/fake exposes to
// force every call to fail as KindUnavailable.
type unreachabler interface {
	Unreachable(bool)
}

// topicSeeder is the optional capability internal/kafka/fake exposes to seed
// a topic's partitions and records (Task 2.1). At Level 2, topics are seeded
// out of band by the acceptance harness, so sub-tests needing it are skipped
// when port doesn't implement it.
type topicSeeder interface {
	SeedTopic(name string, partitions int, records ...kafka.Record)
}

// topicMetaSeeder is the optional capability internal/kafka/fake exposes to
// set a seeded topic's internal flag and replication factor.
type topicMetaSeeder interface {
	SeedTopicMeta(name string, internal bool, replicationFactor int)
}

// brokerConfigSeeder is the optional capability internal/kafka/fake exposes
// to seed a broker's configuration properties.
type brokerConfigSeeder interface {
	SeedBrokerConfigs(brokerID int32, configs ...kafka.ConfigEntry)
}

// topicConfigSeeder is the optional capability internal/kafka/fake exposes to
// seed a topic's configuration properties.
type topicConfigSeeder interface {
	SeedTopicConfigs(name string, configs ...kafka.ConfigEntry)
}

// logDirSeeder is the optional capability internal/kafka/fake exposes to seed
// one partition replica's on-disk log directory and size.
type logDirSeeder interface {
	SeedLogDir(topic string, partition int32, brokerID int32, dir string, bytes int64)
}

// RunAdmin exercises kafka.Admin (FUNC-SPEC §8.1, C1).
func RunAdmin(t *testing.T, port kafka.Admin) {
	t.Helper()

	t.Run("Admin_DescribeCluster_ReturnsSeededBrokers", func(t *testing.T) {
		seeder, ok := port.(adminSeeder)
		if !ok {
			t.Skip("port does not implement the seeding capability")
		}
		seeder.SeedBroker(1, "broker-1", 9092, "rack-a")
		seeder.SeedBroker(2, "broker-2", 9092, "")

		info, err := port.DescribeCluster(context.Background())
		if err != nil {
			t.Fatalf("DescribeCluster() error: %v", err)
		}
		found := map[int32]bool{}
		for _, b := range info.Brokers {
			found[b.ID] = true
		}
		if !found[1] || !found[2] {
			t.Errorf("DescribeCluster() brokers = %+v, missing seeded ids 1 and 2", info.Brokers)
		}
	})

	t.Run("Admin_Unreachable_KindUnavailable", func(t *testing.T) {
		u, ok := port.(unreachabler)
		if !ok {
			t.Skip("port does not implement the unreachable capability")
		}
		u.Unreachable(true)
		defer u.Unreachable(false)

		_, err := port.DescribeCluster(context.Background())
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindUnavailable {
			t.Fatalf("DescribeCluster() while unreachable = %v, want *kafka.Error{Kind: KindUnavailable}", err)
		}
	})

	t.Run("Any_DeadlineExceeded_KindTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()

		_, err := port.DescribeCluster(ctx)
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindTimeout {
			t.Fatalf("DescribeCluster() with an expired context = %v, want *kafka.Error{Kind: KindTimeout}", err)
		}
	})

	t.Run("Admin_DescribeBrokerConfigs_SensitiveHasNilValue", func(t *testing.T) {
		bs, ok := port.(brokerConfigSeeder)
		as, ok2 := port.(adminSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the broker config seeding capability")
		}
		as.SeedBroker(101, "broker-101", 9092, "")
		bs.SeedBrokerConfigs(101,
			kafka.ConfigEntry{Name: "log.retention.ms", Value: "604800000", Source: kafka.SourceDefault},
			kafka.ConfigEntry{Name: "sasl.jaas.config", Value: "", Source: kafka.SourceStatic, IsSensitive: true},
		)

		configs, err := port.DescribeBrokerConfigs(context.Background(), 101)
		if err != nil {
			t.Fatalf("DescribeBrokerConfigs() error: %v", err)
		}
		var found bool
		for _, c := range configs {
			if c.Name != "sasl.jaas.config" {
				continue
			}
			found = true
			if c.Value != "" {
				t.Errorf("sensitive config Value = %q, want empty", c.Value)
			}
			if !c.IsSensitive {
				t.Error("sensitive config IsSensitive = false, want true")
			}
		}
		if !found {
			t.Fatal("DescribeBrokerConfigs() did not return the seeded sensitive config")
		}
	})

	t.Run("Admin_DescribeBrokerConfigs_MissingBrokerNotFound", func(t *testing.T) {
		if _, ok := port.(adminSeeder); !ok {
			t.Skip("missing-broker classification is exercised against the fake only; real-broker kerr mapping is finalised when Level 2 is wired (Task 16.2)")
		}
		_, err := port.DescribeBrokerConfigs(context.Background(), 999999)
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("DescribeBrokerConfigs(missing) = %v, want *kafka.Error{Kind: KindNotFound}", err)
		}
	})

	t.Run("Admin_ListTopics_IncludesInternalFlag", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		tm, ok2 := port.(topicMetaSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the topic seeding capability")
		}
		ts.SeedTopic("t-list-public", 1, kafka.Record{Value: []byte("v")})
		tm.SeedTopicMeta("t-list-public", false, 1)
		ts.SeedTopic("t-list-internal", 1, kafka.Record{Value: []byte("v")})
		tm.SeedTopicMeta("t-list-internal", true, 1)

		topics, err := port.ListTopics(context.Background())
		if err != nil {
			t.Fatalf("ListTopics() error: %v", err)
		}
		byName := map[string]kafka.TopicSummary{}
		for _, s := range topics {
			byName[s.Name] = s
		}
		if pub, ok := byName["t-list-public"]; !ok || pub.Internal {
			t.Errorf("t-list-public = %+v, want present and Internal=false", pub)
		}
		if internal, ok := byName["t-list-internal"]; !ok || !internal.Internal {
			t.Errorf("t-list-internal = %+v, want present and Internal=true", internal)
		}
	})

	t.Run("Admin_DescribeTopics_MissingNotFound", func(t *testing.T) {
		_, err := port.DescribeTopics(context.Background(), "t-does-not-exist")
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("DescribeTopics(missing) = %v, want *kafka.Error{Kind: KindNotFound}", err)
		}
	})

	t.Run("Admin_DescribeTopicConfigs_SourceNormalised", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		tc, ok2 := port.(topicConfigSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the topic config seeding capability")
		}
		ts.SeedTopic("t-configs", 1)
		tc.SeedTopicConfigs("t-configs",
			kafka.ConfigEntry{Name: "cleanup.policy", Value: "delete", Source: kafka.SourceDefault},
			kafka.ConfigEntry{Name: "retention.ms", Value: "3600000", Source: kafka.SourceStatic},
			kafka.ConfigEntry{Name: "compression.type", Value: "producer", Source: kafka.SourceDynamic},
		)

		configs, err := port.DescribeTopicConfigs(context.Background(), "t-configs")
		if err != nil {
			t.Fatalf("DescribeTopicConfigs() error: %v", err)
		}
		gotSource := map[string]kafka.ConfigSource{}
		for _, c := range configs {
			gotSource[c.Name] = c.Source
		}
		want := map[string]kafka.ConfigSource{
			"cleanup.policy":   kafka.SourceDefault,
			"retention.ms":     kafka.SourceStatic,
			"compression.type": kafka.SourceDynamic,
		}
		for name, wantSource := range want {
			if gotSource[name] != wantSource {
				t.Errorf("config %q Source = %v, want %v", name, gotSource[name], wantSource)
			}
		}
	})

	t.Run("Admin_ListOffsets_StartEnd", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		ts.SeedTopic("t-offsets", 2,
			kafka.Record{Partition: 0, Value: []byte("a")},
			kafka.Record{Partition: 0, Value: []byte("b")},
			kafka.Record{Partition: 0, Value: []byte("c")},
			kafka.Record{Partition: 1, Value: []byte("d")},
		)

		start, err := port.ListStartOffsets(context.Background(), "t-offsets")
		if err != nil {
			t.Fatalf("ListStartOffsets() error: %v", err)
		}
		end, err := port.ListEndOffsets(context.Background(), "t-offsets")
		if err != nil {
			t.Fatalf("ListEndOffsets() error: %v", err)
		}
		if start[0] != 0 || start[1] != 0 {
			t.Errorf("start offsets = %v, want {0:0, 1:0}", start)
		}
		if end[0] != 3 || end[1] != 1 {
			t.Errorf("end offsets = %v, want {0:3, 1:1}", end)
		}
	})

	t.Run("Admin_ListOffsetsAfterMilli_FirstAtOrAfter", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		ts.SeedTopic("t-after-milli", 1,
			kafka.Record{Partition: 0, Value: []byte("a"), Timestamp: base},
			kafka.Record{Partition: 0, Value: []byte("b"), Timestamp: base.Add(1 * time.Second)},
			kafka.Record{Partition: 0, Value: []byte("c"), Timestamp: base.Add(2 * time.Second)},
		)

		offsets, err := port.ListOffsetsAfterMilli(context.Background(), "t-after-milli", base.Add(1*time.Second).UnixMilli())
		if err != nil {
			t.Fatalf("ListOffsetsAfterMilli() error: %v", err)
		}
		if offsets[0] != 1 {
			t.Errorf("offset at or after base+1s = %d, want 1", offsets[0])
		}
	})

	t.Run("Admin_DescribeLogDirs_PerReplicaBytes", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		ld, ok2 := port.(logDirSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the log dir seeding capability")
		}
		ts.SeedTopic("t-logdirs", 1, kafka.Record{Value: []byte("v")})
		ld.SeedLogDir("t-logdirs", 0, 1, "/data/kafka-logs", 4096)
		ld.SeedLogDir("t-logdirs", 0, 2, "/data/kafka-logs", 4096)

		entries, err := port.DescribeLogDirs(context.Background(), "t-logdirs")
		if err != nil {
			t.Fatalf("DescribeLogDirs() error: %v", err)
		}
		if len(entries) != 2 {
			t.Fatalf("DescribeLogDirs() = %d entries, want 2", len(entries))
		}
		for _, e := range entries {
			if e.Bytes != 4096 || e.LogDir != "/data/kafka-logs" || e.Partition != 0 {
				t.Errorf("entry = %+v, want {Partition:0, LogDir:/data/kafka-logs, Bytes:4096}", e)
			}
		}
	})
}
