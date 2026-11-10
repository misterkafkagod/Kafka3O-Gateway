package metrics

import (
	"context"
	"slices"
	"time"
)

// GetMetrics reads the cluster's metrics from Kafka; a section Kafka cannot answer carries its error, never zeros.
type GetMetrics interface {
	Execute(ctx context.Context) ClusterMetrics
}

type getMetrics struct {
	logDirs ReadLogDirs
	timeout time.Duration
	now     func() time.Time
}

func NewGetMetrics(logDirs ReadLogDirs, timeout time.Duration, now func() time.Time) GetMetrics {
	return getMetrics{logDirs: logDirs, timeout: timeout, now: now}
}

func (g getMetrics) Execute(ctx context.Context) ClusterMetrics {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	result := ClusterMetrics{ReadAt: g.now().UTC(), LogDirs: LogDirs{Dirs: []LogDir{}}}
	dirs, err := g.logDirs.Execute(ctx)
	if err != nil {
		result.LogDirs.Error = err.Error()
	}
	if dirs != nil {
		result.LogDirs.Dirs = slices.Clone(dirs)
		slices.SortFunc(result.LogDirs.Dirs, CompareLogDirs)
	}
	return result
}
