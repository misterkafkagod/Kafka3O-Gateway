package groups

import "context"

// PreviewOffsetReset computes the offsets a reset would commit, without writing anything.
type PreviewOffsetReset interface {
	Execute(ctx context.Context, group, topic string, target OffsetResetTarget) (OffsetResetPreview, error)
}
