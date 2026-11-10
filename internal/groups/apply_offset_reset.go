package groups

import (
	"context"
	"time"
)

type ApplyOffsetReset interface {
	Execute(ctx context.Context, group, topic string, partitions []OffsetResetRequest) (OffsetResetResult, error)
}

type applyOffsetReset struct {
	commit  CommitOffsetReset
	timeout time.Duration
}

func NewApplyOffsetReset(commit CommitOffsetReset, timeout time.Duration) ApplyOffsetReset {
	return applyOffsetReset{commit: commit, timeout: timeout}
}

func (a applyOffsetReset) Execute(ctx context.Context, group, topic string, partitions []OffsetResetRequest) (OffsetResetResult, error) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	return a.commit.Execute(ctx, group, topic, partitions)
}
