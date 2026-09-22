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
