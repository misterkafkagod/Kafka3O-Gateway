package kafka

import "context"

// Admin is the cluster-administration surface (FUNC-SPEC §8.1: C*, T*, G*, S*
// commands). Methods are added phase by phase in catalog order; every method
// honours ctx cancellation and returns *Error on failure (TECH-SPEC L2, L3).
type Admin interface {
	// DescribeCluster returns the cluster id, controller, and broker list (C1).
	DescribeCluster(ctx context.Context) (ClusterInfo, error)

	// DescribeBrokerConfigs returns one broker's configuration properties,
	// sensitive values blanked (C2). Unknown brokerID → *Error{Kind: NotFound}.
	DescribeBrokerConfigs(ctx context.Context, brokerID int32) ([]ConfigEntry, error)

	// Metadata returns every broker and topic, unfiltered, for the cluster
	// health summary (C4).
	Metadata(ctx context.Context) (ClusterMetadata, error)

	// ListTopics returns every topic's summary, internal topics included
	// (T1). The includeInternal toggle and name-pattern filter are applied
	// by the service layer.
	ListTopics(ctx context.Context) ([]TopicSummary, error)

	// DescribeTopics returns one topic with its partitions' leader, replicas,
	// and ISR (offsets are zero; ListStartOffsets/ListEndOffsets fill them
	// in) (T2). Unknown topic → *Error{Kind: NotFound, Resource: "topic"}.
	DescribeTopics(ctx context.Context, topic string) (Topic, error)

	// DescribeTopicConfigs returns one topic's configuration properties,
	// sensitive values blanked, source normalised (T2).
	DescribeTopicConfigs(ctx context.Context, topic string) ([]ConfigEntry, error)

	// ListStartOffsets returns the oldest (begin) offset per partition (T2, T4).
	ListStartOffsets(ctx context.Context, topic string) (map[int32]int64, error)

	// ListEndOffsets returns the newest (end, exclusive) offset per partition (T2, T4).
	ListEndOffsets(ctx context.Context, topic string) (map[int32]int64, error)

	// ListOffsetsAfterMilli returns, per partition, the offset of the first
	// record at or after millisecond (T4).
	ListOffsetsAfterMilli(ctx context.Context, topic string, millisecond int64) (map[int32]int64, error)

	// DescribeLogDirs returns per-partition, per-replica on-disk byte sizes
	// for one topic (T3).
	DescribeLogDirs(ctx context.Context, topic string) ([]LogDirReplica, error)

	// ListGroups returns every consumer group's state, protocol type, and
	// member count, optionally filtered to the given states — an empty
	// states filters nothing (G1).
	ListGroups(ctx context.Context, states ...string) ([]GroupSummary, error)

	// DescribeGroups returns full detail — members and their assignments —
	// for the named groups, or every group in the cluster when groupIDs is
	// empty (G2, and G3's reverse lookup). Given one or more explicit ids,
	// any that is missing → *Error{Kind: NotFound, Resource: "group"} for
	// the whole call (G2 always asks for exactly one); given none, a group
	// this cluster can't describe is silently omitted rather than failing
	// the whole sweep (G3's reverse lookup is best-effort across every group).
	DescribeGroups(ctx context.Context, groupIDs ...string) ([]Group, error)

	// FetchGroupOffsets returns groupID's committed offset per topic
	// partition (G2 offsets[], G3 lag). Unknown group → *Error{Kind:
	// NotFound, Resource: "group"}.
	FetchGroupOffsets(ctx context.Context, groupID string) (map[TopicPartition]int64, error)

	// CreateTopics creates every spec — or, when validateOnly is true, asks
	// the broker to check they could be created without creating anything
	// (T5's own dryRun; T6's own validate-all-must-not-exist is a
	// service-layer pre-check, FUNC-SPEC §9.4, not this flag). Each spec's
	// own result carries its own error (e.g. AlreadyExists); one bad spec
	// never fails the rest of the batch (T5, T6).
	CreateTopics(ctx context.Context, specs []TopicSpec, validateOnly bool) ([]TopicCreateResult, error)

	// IncrementalAlterTopicConfigs applies changes to topic's configuration
	// (T9). Unknown topic → *Error{Kind: NotFound, Resource: "topic"}.
	IncrementalAlterTopicConfigs(ctx context.Context, topic string, changes []ConfigChange) error

	// CreatePartitions sets topic's partition count to the absolute total
	// (T10; kadm's own CreatePartitions takes a delta instead — this port
	// method deliberately does not, to match T10's `{from, to}` plan
	// directly). Unknown topic → NotFound; total not greater than the
	// current count is checked at the service layer as PARTITION_MISMATCH
	// (FUNC-SPEC §8.6) before this is ever called.
	CreatePartitions(ctx context.Context, topic string, total int32) error

	// DeleteTopics deletes every named topic. Each topic's own result
	// carries its own error (e.g. NotFound); one missing topic never fails
	// the rest of the batch (T7, T8).
	DeleteTopics(ctx context.Context, topics []string) ([]TopicDeleteResult, error)

	// DeleteRecords truncates topic's partitions so each partition's new
	// begin (low watermark) offset is truncateTo[partition] (T11; T12 purge
	// is the service layer calling this with every partition's current end
	// offset). A truncateTo beyond its partition's current end offset is
	// rejected — the service layer normally catches this earlier while
	// building the plan (FUNC-SPEC §8.6), but the port itself still refuses
	// to silently accept it.
	DeleteRecords(ctx context.Context, topic string, truncateTo map[int32]int64) ([]PartitionLowWatermark, error)

	// CommitGroupOffsets commits offsets for group, creating it when it
	// does not yet exist (G4's pre-seed for an absent group). group must
	// currently have no active members, else *Error{Kind: KindGroupActive}.
	CommitGroupOffsets(ctx context.Context, group string, offsets map[TopicPartition]int64) error

	// DeleteGroups deletes every named group (G5). Each group's own result
	// carries its own error (e.g. NotFound, GroupActive for one with live
	// members); one failure never fails the rest of the batch.
	DeleteGroups(ctx context.Context, groups []string) ([]GroupDeleteResult, error)

	// LeaveGroup evicts the named members (by their dynamic member id, the
	// same id G2 reports — not the static group.instance.id KIP-345 uses)
	// from group (G6). Each member's own result carries its own error (e.g.
	// it is not currently part of the group); one failure never fails the
	// rest of the batch.
	LeaveGroup(ctx context.Context, group string, members []string) ([]LeaveGroupResult, error)

	// DescribeQuorum returns the KRaft quorum's current status (C6). A
	// cluster still on ZooKeeper reports *Error{Kind: KindUnsupported}.
	DescribeQuorum(ctx context.Context) (QuorumStatus, error)

	// ListReassignments returns every partition cluster-wide with a
	// reassignment currently in progress (C7).
	ListReassignments(ctx context.Context) ([]PartitionReassignment, error)

	// AlterPartitionAssignments moves each named partition's replicas to
	// the given set (C9 reassign); a nil replica set for a partition
	// cancels its in-progress reassignment instead (C9 cancel). Each
	// partition's own result carries its own error; one failure never
	// fails the rest of the batch.
	AlterPartitionAssignments(ctx context.Context, moves map[TopicPartition][]int32) ([]ReassignResult, error)

	// ElectLeaders triggers a leader election for the given partitions —
	// preferred-replica when preferred is true, unclean otherwise (C9
	// elect). Each partition's own result carries its own error; one
	// failure never fails the rest of the batch.
	ElectLeaders(ctx context.Context, preferred bool, partitions []TopicPartition) ([]ElectLeaderResult, error)

	// IncrementalAlterBrokerConfigs applies changes to brokerID's
	// configuration (C5). Unknown brokerID → *Error{Kind: NotFound}.
	IncrementalAlterBrokerConfigs(ctx context.Context, brokerID int32, changes []ConfigChange) error

	// DescribeAllLogDirs returns every broker's log directory usage,
	// cluster-wide (C8).
	DescribeAllLogDirs(ctx context.Context) ([]BrokerLogDir, error)
}

// Consumer is the message-reading surface (FUNC-SPEC §8.1: M1–M4, M8 source,
// T4, C10). It uses manual partition assignment — no group, no commits
// (FUNC-SPEC O4). The end snapshot a scan reads up to is resolved by the
// caller via Admin.ListEndOffsets, not by Consumer itself (FUNC-SPEC §9.2).
type Consumer interface {
	// Assign begins a manual-assignment session over topic's partitions,
	// each starting at startOffsets[partition] (FUNC-SPEC §9.2 Assign; O4:
	// no group, no commits). Calling Assign again replaces the session.
	Assign(ctx context.Context, topic string, partitions []int32, startOffsets map[int32]int64) error

	// Poll returns whatever records have arrived for the assigned
	// partitions since the last call — possibly none. Poll does not itself
	// know when a partition is exhausted; the caller compares returned
	// offsets against its own end snapshot (FUNC-SPEC §9.2 Poll/Evaluate).
	Poll(ctx context.Context) ([]Record, error)

	// Close releases the session (TECH-SPEC §2.3: a dedicated client per scan).
	Close()
}

// Producer is the message-writing surface (FUNC-SPEC §8.1: M5–M8 target and
// the F5 audit sink).
type Producer interface {
	// Produce writes records to topic, returning one ProduceResult per
	// record in the same order (FUNC-SPEC §8.7 M5). A record with an
	// explicit Partition the topic doesn't have reports that record's own
	// Err; it never fails the rest of the batch.
	Produce(ctx context.Context, topic string, records []ProduceRequest) ([]ProduceResult, error)
}
