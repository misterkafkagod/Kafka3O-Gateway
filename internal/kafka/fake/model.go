package fake

import "github.com/misterkafkagod/kafka3o/internal/kafka"

// model is the in-memory cluster state (TECH-SPEC §4.3 Model row). Phase 1
// models exactly what DescribeCluster, SeedTopic, and SeedGroup need now.
// The topic-internal flag, group members, SCRAM users, client quotas,
// in-progress reassignments, and KRaft quorum state are added by the tasks
// that implement the commands reading them (Tasks 2.1 T1, 7.1 G2, 12.1.3,
// 13.1.2) — nothing here yet reads or sets them.
type model struct {
	clusterID     string
	controllerID  int32
	controllerSet bool
	brokers       []kafka.Broker
	topics        map[string]*fakeTopic
	groups        map[string]*fakeGroup
}

// fakeTopic is one seeded topic and its partitions.
type fakeTopic struct {
	name       string
	partitions []fakePartition
}

// fakePartition is one partition's record log. Offsets are assigned
// sequentially from beginOffset (TECH-SPEC §4.3: "offsets monotonic per
// partition"); compaction is not simulated, so every append is permanent.
type fakePartition struct {
	beginOffset int64
	records     []kafka.Record
}

// fakeGroup is one seeded consumer group.
type fakeGroup struct {
	id      string
	state   string
	offsets map[kafka.TopicPartition]int64
}
