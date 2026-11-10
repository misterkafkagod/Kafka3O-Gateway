package groups

import "context"

// CommitOffsetReset rechecks the preview against Kafka and commits the target offsets.
type CommitOffsetReset interface {
	Execute(ctx context.Context, group, topic string, partitions []OffsetResetRequest) (OffsetResetResult, error)
}
