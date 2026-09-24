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

// groupSeeder is the optional capability internal/kafka/fake exposes to seed
// a consumer group with committed offsets (Task 7.1).
type groupSeeder interface {
	SeedGroup(id string, offsets map[kafka.TopicPartition]int64)
}

// groupMetaSeeder is the optional capability internal/kafka/fake exposes to
// set an already-seeded group's state, protocol type, and coordinator id.
type groupMetaSeeder interface {
	SeedGroupMeta(id, state, protocolType string, coordinatorID int32)
}

// groupMemberSeeder is the optional capability internal/kafka/fake exposes to
// add a live member to an already-seeded group.
type groupMemberSeeder interface {
	SeedGroupMember(id string, member kafka.GroupMember)
}

// quorumSeeder is the optional capability internal/kafka/fake exposes to set
// the KRaft quorum's leader, epoch, voters, and observers (Task 12.1.3).
type quorumSeeder interface {
	SeedQuorum(leaderID, epoch int32, voters, observers []kafka.QuorumReplicaState)
}

// faultInjector is the optional capability internal/kafka/fake exposes to
// fail the next call to method with kind (TECH-SPEC §4.3) — used here to
// simulate a cluster still on ZooKeeper for DescribeQuorum, which has no
// dedicated "unsupported" seeding knob of its own.
type faultInjector interface {
	FailNext(method string, kind kafka.Kind)
}

// RunAdmin exercises kafka.Admin (FUNC-SPEC §8.1, C1).
func RunAdmin(t *testing.T, port kafka.Admin) {
	t.Helper()
	runAdmin(t, port, "")
}

// runAdmin is RunAdmin with every resource the cases create named px +
// "porttest-...", so a run against a shared live cluster (Task 16.2) stays
// inside its own prefix and can be cleaned up by it.
func runAdmin(t *testing.T, port kafka.Admin, px string) {
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

	t.Run("Admin_ListGroups_StateAndMemberCount", func(t *testing.T) {
		gs, ok := port.(groupSeeder)
		gm, ok2 := port.(groupMetaSeeder)
		gmem, ok3 := port.(groupMemberSeeder)
		if !ok || !ok2 || !ok3 {
			t.Skip("port does not implement the group seeding capability")
		}
		gs.SeedGroup("g-list-stable", nil)
		gm.SeedGroupMeta("g-list-stable", "Stable", "consumer", 1)
		gmem.SeedGroupMember("g-list-stable", kafka.GroupMember{MemberID: "m1", ClientID: "c1", Host: "h1"})
		gmem.SeedGroupMember("g-list-stable", kafka.GroupMember{MemberID: "m2", ClientID: "c2", Host: "h2"})
		gs.SeedGroup("g-list-empty", nil)

		groups, err := port.ListGroups(context.Background())
		if err != nil {
			t.Fatalf("ListGroups() error: %v", err)
		}
		byID := map[string]kafka.GroupSummary{}
		for _, g := range groups {
			byID[g.ID] = g
		}
		stable, ok := byID["g-list-stable"]
		if !ok || stable.State != "Stable" || stable.ProtocolType != "consumer" || stable.MemberCount != 2 {
			t.Errorf("g-list-stable = %+v, want {State:Stable ProtocolType:consumer MemberCount:2}", stable)
		}
		if empty, ok := byID["g-list-empty"]; !ok || empty.MemberCount != 0 {
			t.Errorf("g-list-empty = %+v, want MemberCount 0", empty)
		}

		filtered, err := port.ListGroups(context.Background(), "Stable")
		if err != nil {
			t.Fatalf("ListGroups(Stable) error: %v", err)
		}
		if len(filtered) == 0 {
			t.Fatal("ListGroups(Stable) = [], want at least g-list-stable")
		}
		for _, g := range filtered {
			if g.State != "Stable" {
				t.Errorf("ListGroups(Stable) returned %+v, want only Stable groups", g)
			}
		}
	})

	t.Run("Admin_DescribeGroups_MissingIsNotFound", func(t *testing.T) {
		if _, ok := port.(groupSeeder); !ok {
			t.Skip("missing-group classification is exercised against the fake only; real-group kerr mapping is finalised when Level 2 is wired")
		}
		_, err := port.DescribeGroups(context.Background(), "g-does-not-exist")
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("DescribeGroups(missing) = %v, want *kafka.Error{Kind: KindNotFound}", err)
		}
	})

	t.Run("Admin_FetchGroupOffsets_PerPartition", func(t *testing.T) {
		gs, ok := port.(groupSeeder)
		if !ok {
			t.Skip("port does not implement the group seeding capability")
		}
		gs.SeedGroup("g-offsets", map[kafka.TopicPartition]int64{
			{Topic: "t-a", Partition: 0}: 10,
			{Topic: "t-a", Partition: 1}: 20,
			{Topic: "t-b", Partition: 0}: 5,
		})

		offsets, err := port.FetchGroupOffsets(context.Background(), "g-offsets")
		if err != nil {
			t.Fatalf("FetchGroupOffsets() error: %v", err)
		}
		want := map[kafka.TopicPartition]int64{
			{Topic: "t-a", Partition: 0}: 10,
			{Topic: "t-a", Partition: 1}: 20,
			{Topic: "t-b", Partition: 0}: 5,
		}
		if len(offsets) != len(want) {
			t.Fatalf("FetchGroupOffsets() = %v, want %v", offsets, want)
		}
		for tp, wantOffset := range want {
			if offsets[tp] != wantOffset {
				t.Errorf("FetchGroupOffsets()[%v] = %d, want %d", tp, offsets[tp], wantOffset)
			}
		}
	})

	t.Run("Admin_FetchGroupOffsets_MissingGroupNotFound", func(t *testing.T) {
		if _, ok := port.(groupSeeder); !ok {
			t.Skip("missing-group classification is exercised against the fake only; real-group kerr mapping is finalised when Level 2 is wired")
		}
		_, err := port.FetchGroupOffsets(context.Background(), "g-does-not-exist")
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("FetchGroupOffsets(missing) = %v, want *kafka.Error{Kind: KindNotFound}", err)
		}
	})

	// CreateTopics/IncrementalAlterTopicConfigs/CreatePartitions are
	// self-contained (each case creates whatever topic it needs via
	// CreateTopics itself), so they run against any Admin unconditionally —
	// no seeding-capability skip guard, unlike the read-only cases above
	// whose fixtures the fake alone can seed out of band.

	t.Run("Admin_CreateTopics_ValidateOnlyCreatesNothing", func(t *testing.T) {
		name := px + "porttest-validate-only"
		results, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 1, ReplicationFactor: 1},
		}, true)
		if err != nil {
			t.Fatalf("CreateTopics(validateOnly) error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("CreateTopics(validateOnly) = %+v, want one validated result, no error", results)
		}

		_, err = port.DescribeTopics(context.Background(), name)
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("DescribeTopics(%q) after validateOnly = %v, want NotFound (nothing was created)", name, err)
		}
	})

	t.Run("Admin_CreateTopics_ExistingIsAlreadyExists", func(t *testing.T) {
		name := px + "porttest-already-exists"
		if _, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 1, ReplicationFactor: 1},
		}, false); err != nil {
			t.Fatalf("CreateTopics() error: %v", err)
		}

		results, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 1, ReplicationFactor: 1},
		}, false)
		if err != nil {
			t.Fatalf("CreateTopics(existing) error: %v", err)
		}
		if len(results) != 1 || !kafka.IsKind(results[0].Err, kafka.KindAlreadyExists) {
			t.Fatalf("CreateTopics(existing) = %+v, want one AlreadyExists result", results)
		}
	})

	t.Run("Admin_IncrementalAlterTopicConfigs_SetAndResetToDefault", func(t *testing.T) {
		name := px + "porttest-alter-config"
		if _, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 1, ReplicationFactor: 1},
		}, false); err != nil {
			t.Fatalf("CreateTopics() error: %v", err)
		}

		newValue := "60000"
		if err := port.IncrementalAlterTopicConfigs(context.Background(), name, []kafka.ConfigChange{
			{Name: "retention.ms", Value: &newValue},
		}); err != nil {
			t.Fatalf("IncrementalAlterTopicConfigs(set) error: %v", err)
		}
		configs, err := port.DescribeTopicConfigs(context.Background(), name)
		if err != nil {
			t.Fatalf("DescribeTopicConfigs() error: %v", err)
		}
		found := false
		for _, c := range configs {
			if c.Name != "retention.ms" {
				continue
			}
			found = true
			if c.Value != "60000" || c.Source != kafka.SourceDynamic {
				t.Errorf("retention.ms after set = %+v, want Value 60000, Source dynamic", c)
			}
		}
		if !found {
			t.Fatal("retention.ms missing after set")
		}

		if err := port.IncrementalAlterTopicConfigs(context.Background(), name, []kafka.ConfigChange{
			{Name: "retention.ms", Value: nil},
		}); err != nil {
			t.Fatalf("IncrementalAlterTopicConfigs(reset) error: %v", err)
		}
		configs, err = port.DescribeTopicConfigs(context.Background(), name)
		if err != nil {
			t.Fatalf("DescribeTopicConfigs() after reset error: %v", err)
		}
		for _, c := range configs {
			if c.Name == "retention.ms" && c.Source != kafka.SourceDefault {
				t.Errorf("retention.ms after reset = %+v, want Source default", c)
			}
		}
	})

	t.Run("Admin_CreatePartitions_IncreaseSucceeds", func(t *testing.T) {
		name := px + "porttest-add-partitions"
		if _, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 1, ReplicationFactor: 1},
		}, false); err != nil {
			t.Fatalf("CreateTopics() error: %v", err)
		}

		if err := port.CreatePartitions(context.Background(), name, 4); err != nil {
			t.Fatalf("CreatePartitions(increase) error: %v", err)
		}
		topic, err := port.DescribeTopics(context.Background(), name)
		if err != nil {
			t.Fatalf("DescribeTopics() error: %v", err)
		}
		if len(topic.Partitions) != 4 {
			t.Errorf("Partitions = %d, want 4", len(topic.Partitions))
		}
	})

	t.Run("Admin_CreatePartitions_DecreaseIsError", func(t *testing.T) {
		name := px + "porttest-decrease-partitions"
		if _, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 4, ReplicationFactor: 1},
		}, false); err != nil {
			t.Fatalf("CreateTopics() error: %v", err)
		}

		if err := port.CreatePartitions(context.Background(), name, 1); err == nil {
			t.Fatal("CreatePartitions(decrease) = nil, want an error")
		}
	})

	t.Run("Admin_DeleteTopics_RemovesTopic", func(t *testing.T) {
		name := px + "porttest-delete-topic"
		if _, err := port.CreateTopics(context.Background(), []kafka.TopicSpec{
			{Name: name, Partitions: 1, ReplicationFactor: 1},
		}, false); err != nil {
			t.Fatalf("CreateTopics() error: %v", err)
		}

		results, err := port.DeleteTopics(context.Background(), []string{name})
		if err != nil {
			t.Fatalf("DeleteTopics() error: %v", err)
		}
		if len(results) != 1 || results[0].Name != name || results[0].Err != nil {
			t.Fatalf("DeleteTopics() results = %+v, want one clean result for %q", results, name)
		}

		if _, err := port.DescribeTopics(context.Background(), name); err == nil {
			t.Error("DescribeTopics() after delete = nil error, want NotFound")
		}
	})

	t.Run("Admin_DeleteTopics_MissingIsNotFound", func(t *testing.T) {
		results, err := port.DeleteTopics(context.Background(), []string{px + "porttest-delete-missing"})
		if err != nil {
			t.Fatalf("DeleteTopics() error: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("DeleteTopics() results = %+v, want exactly one", results)
		}
		var ke *kafka.Error
		if !errors.As(results[0].Err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Errorf("DeleteTopics() result.Err = %v, want *kafka.Error{Kind: KindNotFound}", results[0].Err)
		}
	})

	t.Run("Admin_DeleteRecords_RaisesBeginOffset", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		ts.SeedTopic(px+"porttest-delete-records", 1,
			kafka.Record{Partition: 0, Value: []byte("a")},
			kafka.Record{Partition: 0, Value: []byte("b")},
			kafka.Record{Partition: 0, Value: []byte("c")},
			kafka.Record{Partition: 0, Value: []byte("d")},
		)

		results, err := port.DeleteRecords(context.Background(), px+"porttest-delete-records", map[int32]int64{0: 2})
		if err != nil {
			t.Fatalf("DeleteRecords() error: %v", err)
		}
		if len(results) != 1 || results[0].Partition != 0 || results[0].LowWatermark != 2 {
			t.Fatalf("DeleteRecords() results = %+v, want one {partition:0 lowWatermark:2}", results)
		}

		start, err := port.ListStartOffsets(context.Background(), px+"porttest-delete-records")
		if err != nil {
			t.Fatalf("ListStartOffsets() error: %v", err)
		}
		if start[0] != 2 {
			t.Errorf("ListStartOffsets()[0] = %d, want 2 (begin offset raised)", start[0])
		}
	})

	t.Run("Admin_DeleteRecords_BeyondEndIsError", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		ts.SeedTopic(px+"porttest-delete-records-beyond-end", 1,
			kafka.Record{Partition: 0, Value: []byte("a")},
		)

		if _, err := port.DeleteRecords(context.Background(), px+"porttest-delete-records-beyond-end", map[int32]int64{0: 999}); err == nil {
			t.Fatal("DeleteRecords(truncateTo beyond end) = nil, want an error")
		}
	})

	t.Run("Admin_CommitGroupOffsets_CreatesAbsentGroup", func(t *testing.T) {
		name := px + "porttest-commit-absent"
		offsets := map[kafka.TopicPartition]int64{{Topic: "t", Partition: 0}: 5}

		if err := port.CommitGroupOffsets(context.Background(), name, offsets); err != nil {
			t.Fatalf("CommitGroupOffsets() error: %v", err)
		}

		got, err := port.FetchGroupOffsets(context.Background(), name)
		if err != nil {
			t.Fatalf("FetchGroupOffsets() error: %v", err)
		}
		if got[kafka.TopicPartition{Topic: "t", Partition: 0}] != 5 {
			t.Errorf("FetchGroupOffsets() = %v, want {t/0: 5}", got)
		}
	})

	t.Run("Admin_CommitGroupOffsets_ActiveGroupIsGroupActive", func(t *testing.T) {
		gs, ok := port.(groupSeeder)
		ms, ok2 := port.(groupMemberSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the group seeding capabilities")
		}
		name := px + "porttest-commit-active"
		gs.SeedGroup(name, nil)
		ms.SeedGroupMember(name, kafka.GroupMember{MemberID: "m1"})

		err := port.CommitGroupOffsets(context.Background(), name, map[kafka.TopicPartition]int64{{Topic: "t", Partition: 0}: 5})
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindGroupActive {
			t.Fatalf("CommitGroupOffsets(active group) error = %v, want *kafka.Error{Kind: KindGroupActive}", err)
		}
	})

	t.Run("Admin_DeleteGroups_ActiveIsGroupActive", func(t *testing.T) {
		gs, ok := port.(groupSeeder)
		ms, ok2 := port.(groupMemberSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the group seeding capabilities")
		}
		name := px + "porttest-delete-group-active"
		gs.SeedGroup(name, nil)
		ms.SeedGroupMember(name, kafka.GroupMember{MemberID: "m1"})

		results, err := port.DeleteGroups(context.Background(), []string{name})
		if err != nil {
			t.Fatalf("DeleteGroups() error: %v", err)
		}
		if len(results) != 1 {
			t.Fatalf("DeleteGroups() results = %+v, want exactly one", results)
		}
		var ke *kafka.Error
		if !errors.As(results[0].Err, &ke) || ke.Kind != kafka.KindGroupActive {
			t.Errorf("DeleteGroups() result.Err = %v, want *kafka.Error{Kind: KindGroupActive}", results[0].Err)
		}
	})

	t.Run("Admin_DeleteGroups_InactiveRemoved", func(t *testing.T) {
		gs, ok := port.(groupSeeder)
		if !ok {
			t.Skip("port does not implement the group seeding capability")
		}
		name := px + "porttest-delete-group-inactive"
		gs.SeedGroup(name, nil)

		results, err := port.DeleteGroups(context.Background(), []string{name})
		if err != nil {
			t.Fatalf("DeleteGroups() error: %v", err)
		}
		if len(results) != 1 || results[0].ID != name || results[0].Err != nil {
			t.Fatalf("DeleteGroups() results = %+v, want one clean result for %q", results, name)
		}

		if _, err := port.DescribeGroups(context.Background(), name); err == nil {
			t.Error("DescribeGroups() after delete = nil error, want NotFound")
		}
	})

	t.Run("Admin_LeaveGroup_RemovesListedMembers", func(t *testing.T) {
		gs, ok := port.(groupSeeder)
		ms, ok2 := port.(groupMemberSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the group seeding capabilities")
		}
		name := px + "porttest-leave-group"
		gs.SeedGroup(name, nil)
		ms.SeedGroupMember(name, kafka.GroupMember{MemberID: "m1"})
		ms.SeedGroupMember(name, kafka.GroupMember{MemberID: "m2"})

		results, err := port.LeaveGroup(context.Background(), name, []string{"m1"})
		if err != nil {
			t.Fatalf("LeaveGroup() error: %v", err)
		}
		if len(results) != 1 || results[0].MemberID != "m1" || results[0].Err != nil {
			t.Fatalf("LeaveGroup() results = %+v, want one clean result for m1", results)
		}

		groups, err := port.DescribeGroups(context.Background(), name)
		if err != nil {
			t.Fatalf("DescribeGroups() error: %v", err)
		}
		if len(groups) != 1 {
			t.Fatalf("DescribeGroups() = %+v, want exactly one group", groups)
		}
		found := map[string]bool{}
		for _, m := range groups[0].Members {
			found[m.MemberID] = true
		}
		if found["m1"] || !found["m2"] {
			t.Errorf("group members = %v, want m1 removed and m2 remaining", groups[0].Members)
		}
	})

	t.Run("Admin_DescribeQuorum_LeaderAndVoters", func(t *testing.T) {
		qs, ok := port.(quorumSeeder)
		if !ok {
			t.Skip("port does not implement the quorum seeding capability")
		}
		qs.SeedQuorum(1, 5,
			[]kafka.QuorumReplicaState{{ID: 1, LogEndOffset: 100}, {ID: 2, LogEndOffset: 98, LagMs: 50}},
			[]kafka.QuorumReplicaState{{ID: 3, LogEndOffset: 90, LagMs: 200}},
		)

		status, err := port.DescribeQuorum(context.Background())
		if err != nil {
			t.Fatalf("DescribeQuorum() error: %v", err)
		}
		if status.LeaderID != 1 || status.Epoch != 5 || len(status.Voters) != 2 || len(status.Observers) != 1 {
			t.Fatalf("DescribeQuorum() = %+v, want LeaderID 1, Epoch 5, 2 voters, 1 observer", status)
		}
	})

	t.Run("Admin_DescribeQuorum_UnsupportedIsKindUnsupported", func(t *testing.T) {
		fi, ok := port.(faultInjector)
		if !ok {
			t.Skip("port does not implement the fault injection capability")
		}
		fi.FailNext("DescribeQuorum", kafka.KindUnsupported)

		_, err := port.DescribeQuorum(context.Background())
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindUnsupported {
			t.Fatalf("DescribeQuorum() error = %v, want *kafka.Error{Kind: KindUnsupported}", err)
		}
	})

	t.Run("Admin_Reassignments_ListAfterAlter", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		name := px + "porttest-reassign-list"
		ts.SeedTopic(name, 1)

		results, err := port.AlterPartitionAssignments(context.Background(), map[kafka.TopicPartition][]int32{
			{Topic: name, Partition: 0}: {1, 2, 3},
		})
		if err != nil {
			t.Fatalf("AlterPartitionAssignments() error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("AlterPartitionAssignments() results = %+v, want one clean result", results)
		}

		reassignments, err := port.ListReassignments(context.Background())
		if err != nil {
			t.Fatalf("ListReassignments() error: %v", err)
		}
		found := false
		for _, r := range reassignments {
			if r.Topic == name && r.Partition == 0 {
				found = true
			}
		}
		if !found {
			t.Errorf("ListReassignments() = %+v, want %s/0 listed", reassignments, name)
		}
	})

	t.Run("Admin_Reassignments_Cancel", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		name := px + "porttest-reassign-cancel"
		ts.SeedTopic(name, 1)

		if _, err := port.AlterPartitionAssignments(context.Background(), map[kafka.TopicPartition][]int32{
			{Topic: name, Partition: 0}: {1, 2, 3},
		}); err != nil {
			t.Fatalf("AlterPartitionAssignments(move) error: %v", err)
		}
		if _, err := port.AlterPartitionAssignments(context.Background(), map[kafka.TopicPartition][]int32{
			{Topic: name, Partition: 0}: nil,
		}); err != nil {
			t.Fatalf("AlterPartitionAssignments(cancel) error: %v", err)
		}

		reassignments, err := port.ListReassignments(context.Background())
		if err != nil {
			t.Fatalf("ListReassignments() error: %v", err)
		}
		for _, r := range reassignments {
			if r.Topic == name && r.Partition == 0 {
				t.Errorf("ListReassignments() still lists %s/0 after cancel", name)
			}
		}
	})

	t.Run("Admin_ElectLeaders_PreferredAndUnclean", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		if !ok {
			t.Skip("port does not implement the topic seeding capability")
		}
		name := px + "porttest-elect"
		ts.SeedTopic(name, 1)
		if _, err := port.AlterPartitionAssignments(context.Background(), map[kafka.TopicPartition][]int32{
			{Topic: name, Partition: 0}: {2, 1},
		}); err != nil {
			t.Fatalf("AlterPartitionAssignments() error: %v", err)
		}

		for _, preferred := range []bool{true, false} {
			results, err := port.ElectLeaders(context.Background(), preferred, []kafka.TopicPartition{{Topic: name, Partition: 0}})
			if err != nil {
				t.Fatalf("ElectLeaders(preferred=%v) error: %v", preferred, err)
			}
			if len(results) != 1 || results[0].Err != nil {
				t.Fatalf("ElectLeaders(preferred=%v) results = %+v, want one clean result", preferred, results)
			}
		}
	})

	t.Run("Admin_IncrementalAlterBrokerConfigs_SetAndReset", func(t *testing.T) {
		as, ok := port.(adminSeeder)
		bs, ok2 := port.(brokerConfigSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the broker seeding capabilities")
		}
		as.SeedBroker(101, "broker-101", 9092, "")
		bs.SeedBrokerConfigs(101, kafka.ConfigEntry{Name: "log.retention.hours", Value: "168", Source: kafka.SourceDefault})

		newValue := "72"
		if err := port.IncrementalAlterBrokerConfigs(context.Background(), 101, []kafka.ConfigChange{
			{Name: "log.retention.hours", Value: &newValue},
		}); err != nil {
			t.Fatalf("IncrementalAlterBrokerConfigs(set) error: %v", err)
		}
		configs, err := port.DescribeBrokerConfigs(context.Background(), 101)
		if err != nil {
			t.Fatalf("DescribeBrokerConfigs() error: %v", err)
		}
		found := false
		for _, c := range configs {
			if c.Name == "log.retention.hours" {
				found = true
				if c.Value != "72" || c.Source != kafka.SourceDynamic {
					t.Errorf("log.retention.hours = %+v, want value 72 source dynamic", c)
				}
			}
		}
		if !found {
			t.Fatal("log.retention.hours missing after set")
		}

		if err := port.IncrementalAlterBrokerConfigs(context.Background(), 101, []kafka.ConfigChange{
			{Name: "log.retention.hours"},
		}); err != nil {
			t.Fatalf("IncrementalAlterBrokerConfigs(reset) error: %v", err)
		}
		configs, err = port.DescribeBrokerConfigs(context.Background(), 101)
		if err != nil {
			t.Fatalf("DescribeBrokerConfigs() error: %v", err)
		}
		for _, c := range configs {
			if c.Name == "log.retention.hours" && c.Source != kafka.SourceDefault {
				t.Errorf("log.retention.hours.Source after reset = %v, want default", c.Source)
			}
		}
	})

	t.Run("Admin_DescribeLogDirs_AllBrokers", func(t *testing.T) {
		ts, ok := port.(topicSeeder)
		lds, ok2 := port.(logDirSeeder)
		if !ok || !ok2 {
			t.Skip("port does not implement the topic/log-dir seeding capabilities")
		}
		name := px + "porttest-alldirs"
		ts.SeedTopic(name, 1)
		lds.SeedLogDir(name, 0, 1, "/var/kafka/data", 1024)

		dirs, err := port.DescribeAllLogDirs(context.Background())
		if err != nil {
			t.Fatalf("DescribeAllLogDirs() error: %v", err)
		}
		found := false
		for _, d := range dirs {
			if d.BrokerID == 1 && d.LogDir == "/var/kafka/data" && d.TotalBytes >= 1024 {
				found = true
			}
		}
		if !found {
			t.Errorf("DescribeAllLogDirs() = %+v, want a broker 1 /var/kafka/data entry with totalBytes >= 1024", dirs)
		}
	})

	t.Run("Admin_SCRAM_UpsertThenDescribeNoSecret", func(t *testing.T) {
		user := px + "porttest-scram-upsert"
		results, err := port.AlterUserSCRAMs(context.Background(), []kafka.ScramUpsert{
			{User: user, Mechanism: kafka.ScramSha256, Iterations: 4096, Password: "s3cret"},
		}, nil)
		if err != nil {
			t.Fatalf("AlterUserSCRAMs(upsert) error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("AlterUserSCRAMs(upsert) results = %+v, want one clean result", results)
		}

		// ScramUser/ScramCredential carry mechanism and iterations only — no
		// Password/Salt field exists on the port's types to leak through
		// (TECH-SPEC C7), so a successful describe is itself the "no secret"
		// proof: there is no field to assert absent.
		users, err := port.DescribeUserSCRAMs(context.Background(), user)
		if err != nil {
			t.Fatalf("DescribeUserSCRAMs() error: %v", err)
		}
		if len(users) != 1 || users[0].Name != user {
			t.Fatalf("DescribeUserSCRAMs() = %+v, want one entry for %s", users, user)
		}
		if len(users[0].Credentials) != 1 || users[0].Credentials[0].Mechanism != kafka.ScramSha256 || users[0].Credentials[0].Iterations != 4096 {
			t.Errorf("DescribeUserSCRAMs() credentials = %+v, want SCRAM-SHA-256 iterations 4096", users[0].Credentials)
		}
	})

	t.Run("Admin_SCRAM_DeleteRemoves", func(t *testing.T) {
		user := px + "porttest-scram-delete"
		if _, err := port.AlterUserSCRAMs(context.Background(), []kafka.ScramUpsert{
			{User: user, Mechanism: kafka.ScramSha256, Iterations: 4096, Password: "s3cret"},
		}, nil); err != nil {
			t.Fatalf("AlterUserSCRAMs(upsert) error: %v", err)
		}

		results, err := port.AlterUserSCRAMs(context.Background(), nil, []kafka.ScramDelete{
			{User: user, Mechanism: kafka.ScramSha256},
		})
		if err != nil {
			t.Fatalf("AlterUserSCRAMs(delete) error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("AlterUserSCRAMs(delete) results = %+v, want one clean result", results)
		}

		_, err = port.DescribeUserSCRAMs(context.Background(), user)
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("DescribeUserSCRAMs() after delete error = %v, want *kafka.Error{Kind: KindNotFound}", err)
		}
	})

	t.Run("Admin_SCRAM_DescribeMissingIsNotFound", func(t *testing.T) {
		_, err := port.DescribeUserSCRAMs(context.Background(), px+"porttest-scram-missing")
		var ke *kafka.Error
		if !errors.As(err, &ke) || ke.Kind != kafka.KindNotFound {
			t.Fatalf("DescribeUserSCRAMs() error = %v, want *kafka.Error{Kind: KindNotFound}", err)
		}
	})

	t.Run("Admin_Quotas_AlterThenDescribe", func(t *testing.T) {
		user := px + "porttest-quota-user"
		entity := kafka.QuotaEntity{{Type: "user", Name: &user}}
		results, err := port.AlterClientQuotas(context.Background(), []kafka.QuotaAlterEntry{
			{Entity: entity, Ops: []kafka.QuotaOp{{Key: "producer_byte_rate", Value: 1048576}}},
		})
		if err != nil {
			t.Fatalf("AlterClientQuotas(set) error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("AlterClientQuotas(set) results = %+v, want one clean result", results)
		}

		quotas, err := port.DescribeClientQuotas(context.Background(), "user")
		if err != nil {
			t.Fatalf("DescribeClientQuotas() error: %v", err)
		}
		found := false
		for _, q := range quotas {
			for _, c := range q.Entity {
				if c.Type != "user" || c.Name == nil || *c.Name != user {
					continue
				}
				for _, v := range q.Values {
					if v.Key == "producer_byte_rate" && v.Value == 1048576 {
						found = true
					}
				}
			}
		}
		if !found {
			t.Errorf("DescribeClientQuotas() = %+v, want %s producer_byte_rate=1048576", quotas, user)
		}
	})

	t.Run("Admin_Quotas_RemoveKey", func(t *testing.T) {
		user := px + "porttest-quota-removekey"
		entity := kafka.QuotaEntity{{Type: "user", Name: &user}}
		if _, err := port.AlterClientQuotas(context.Background(), []kafka.QuotaAlterEntry{
			{Entity: entity, Ops: []kafka.QuotaOp{{Key: "producer_byte_rate", Value: 2097152}}},
		}); err != nil {
			t.Fatalf("AlterClientQuotas(set) error: %v", err)
		}

		results, err := port.AlterClientQuotas(context.Background(), []kafka.QuotaAlterEntry{
			{Entity: entity, Ops: []kafka.QuotaOp{{Key: "producer_byte_rate", Remove: true}}},
		})
		if err != nil {
			t.Fatalf("AlterClientQuotas(remove) error: %v", err)
		}
		if len(results) != 1 || results[0].Err != nil {
			t.Fatalf("AlterClientQuotas(remove) results = %+v, want one clean result", results)
		}

		quotas, err := port.DescribeClientQuotas(context.Background(), "user")
		if err != nil {
			t.Fatalf("DescribeClientQuotas() error: %v", err)
		}
		for _, q := range quotas {
			for _, c := range q.Entity {
				if c.Type == "user" && c.Name != nil && *c.Name == user {
					t.Errorf("DescribeClientQuotas() still lists %s after removing its only key", user)
				}
			}
		}
	})
}
