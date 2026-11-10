package metrics

import "context"

// ReadEndOffsets sums each topic's partition end offsets, sorted by topic.
type ReadEndOffsets interface {
	Execute(ctx context.Context, topics []string) ([]TopicEndOffset, error)
}
