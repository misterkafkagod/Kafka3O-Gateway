package fake

import "github.com/misterkafkagod/kafka3o/internal/kafka"

// model is the in-memory cluster state (TECH-SPEC §4.3 Model row). Phase 1,
// Task 2.1, Task 7.1, Task 12.1.3, and Task 13.1.2 model what DescribeCluster,
// the inspection commands (C2, C4, T1-T4), the group commands (G1-G3), the
// advanced cluster commands (C5-C9), and the security commands (S1, S2) need.
type model struct {
	clusterID     string
	controllerID  int32
	controllerSet bool
	brokers       []kafka.Broker
	brokerConfigs map[int32][]kafka.ConfigEntry
	topics        map[string]*fakeTopic
	groups        map[string]*fakeGroup
	quorum        fakeQuorum
	scramUsers    map[string]*fakeScramUser
	quotas        map[string]*fakeQuota
}

// fakeScramUser is one user's configured SCRAM credentials, keyed by
// mechanism. It stores mechanism and iteration count only — never a
// password, salt, or salted password (Task 13.1.2: "no secrets stored";
// TestFake_SCRAM_StoresNoPasswordMaterial asserts this at the type level).
type fakeScramUser struct {
	name        string
	credentials map[kafka.ScramMechanism]int32
}

// fakeQuota is one entity's configured quota values, keyed by the entity
// descriptor toEntityKey builds from it (Task 13.1.2 S2).
type fakeQuota struct {
	entity kafka.QuotaEntity
	values map[string]float64
}

// fakeQuorum is the seeded KRaft quorum status (Task 12.1.3 C6). Its zero
// value describes an empty quorum (leader 0, no voters) — DescribeQuorum
// never itself reports KindUnsupported; a test simulating a ZooKeeper-mode
// cluster does so via FailNext/FailAlways("DescribeQuorum", KindUnsupported),
// the same fault-injection primitives every other command uses (TECH-SPEC §4.3).
type fakeQuorum struct {
	leaderID  int32
	epoch     int32
	voters    []kafka.QuorumReplicaState
	observers []kafka.QuorumReplicaState
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
	// addingReplicas/removingReplicas are non-nil while a reassignment this
	// partition is mid-flight (Task 12.1.3 C7, C9): AlterPartitionAssignments
	// sets them and applies the new replica set immediately (the fake has no
	// background ISR-catch-up process to simulate), leaving them populated
	// until a later AlterPartitionAssignments call — real or cancelling —
	// clears them.
	addingReplicas   []int32
	removingReplicas []int32
}

// fakeLogDirEntry is one replica's seeded log directory and size.
type fakeLogDirEntry struct {
	dir   string
	bytes int64
}

// fakeGroup is one seeded consumer group.
type fakeGroup struct {
	id            string
	state         string
	protocolType  string
	coordinatorID int32
	offsets       map[kafka.TopicPartition]int64
	members       []kafka.GroupMember
}
