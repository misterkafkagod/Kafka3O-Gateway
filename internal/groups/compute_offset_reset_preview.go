package groups

import "context"

// ComputeOffsetResetPreview reads Kafka to compute the offsets a reset would commit, without writing anything.
type ComputeOffsetResetPreview interface {
	Execute(ctx context.Context, group, topic string, target OffsetResetTarget) (OffsetResetPreview, error)
}
