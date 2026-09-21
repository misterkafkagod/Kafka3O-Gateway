package fake

import "github.com/misterkafkagod/kafka3o/internal/kafka"

// model is the in-memory cluster state (TECH-SPEC §4.3 Model row). Phase 1
// and Task 2.1 model what DescribeCluster, the inspection commands (C2, C4,
// T1-T4), SeedTopic, and SeedGroup need now. Group members, SCRAM users,
// client quotas, in-progress reassignments, and KRaft quorum state are added
// by the tasks that implement the commands reading them (Tasks 7.1 G2,
// 12.1.3, 13.1.2) — nothing here yet reads or sets them.
type model struct {
	clusterID     string
	controllerID  int32
	controllerSet bool
	brokers       []kafka.Broker
	brokerConfigs map[int32][]kafka.ConfigEntry
	topics        map[string]*fakeTopic
	groups        map[string]*fakeGroup
}

// fakeTopic is one seeded topic and its partitions.
type fakeTopic struct {
	name              string
	internal          bool
	replicationFactor int
	partitions        []fakePartition
	configs           []kafka.ConfigEntry
}

// fakePartition is one partition's record log. Offsets are assigned
// sequentially from beginOffset (TECH-SPEC §4.3: "offsets monotonic per
// partition"); compaction is not simulated, so every append is permanent.
// leader/replicas/isr and logDir default to zero values for topics seeded
// without them (Task 2.1's describe/config tests seed them explicitly).
type fakePartition struct {
	beginOffset int64
	records     []kafka.Record

	leader   int32
	replicas []int32
	isr      []int32
	// logDir maps a replica's broker id to its on-disk footprint (Task 2.1 T3).
	logDir map[int32]fakeLogDirEntry
}

// fakeLogDirEntry is one replica's seeded log directory and size.
type fakeLogDirEntry struct {
	dir   string
	bytes int64
}

// fakeGroup is one seeded consumer group.
type fakeGroup struct {
	id      string
	state   string
	offsets map[kafka.TopicPartition]int64
}
