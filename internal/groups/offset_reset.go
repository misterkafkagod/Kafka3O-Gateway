package groups

import "errors"

// OffsetResetTarget chooses where a reset moves each partition: "earliest", "latest", or "timestamp" with
// Timestamp in Unix milliseconds.
type OffsetResetTarget struct {
	Kind      string
	Timestamp int64
}

type PartitionOffsetReset struct {
	Partition int32
	// CurrentOffset is nil when the group has no commit for the partition.
	CurrentOffset *int64
	TargetOffset  int64
}

type OffsetResetPreview struct {
	Group      string
	Topic      string
	Partitions []PartitionOffsetReset
}

var (
	ErrGroupActive        = errors.New("the consumer group has active members")
	ErrGroupNotFound      = errors.New("the consumer group does not exist")
	ErrTopicNotFound      = errors.New("the topic does not exist")
	ErrInvalidResetTarget = errors.New("the offset reset target is invalid")
)
