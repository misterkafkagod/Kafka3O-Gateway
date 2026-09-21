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
// (FUNC-SPEC O4). Methods arrive with Phase 3 (Task 3.1).
type Consumer interface{}

// Producer is the message-writing surface (FUNC-SPEC §8.1: M5–M8 target and
// the F5 audit sink). Methods arrive with Phase 5 (Task 5.1).
type Producer interface{}
