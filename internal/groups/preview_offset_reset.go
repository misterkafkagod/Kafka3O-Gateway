package groups

import (
	"context"
	"time"
)

type PreviewOffsetReset interface {
	Execute(ctx context.Context, group, topic string, target OffsetResetTarget) (OffsetResetPreview, error)
}

type previewOffsetReset struct {
	compute ComputeOffsetResetPreview
	timeout time.Duration
}

func NewPreviewOffsetReset(compute ComputeOffsetResetPreview, timeout time.Duration) PreviewOffsetReset {
	return previewOffsetReset{compute: compute, timeout: timeout}
}

func (p previewOffsetReset) Execute(ctx context.Context, group, topic string, target OffsetResetTarget) (OffsetResetPreview, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.compute.Execute(ctx, group, topic, target)
}
