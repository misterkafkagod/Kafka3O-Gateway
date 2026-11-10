package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/metrics"
)

type logDirsAdmin interface {
	DescribeAllLogDirs(context.Context, kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
}

type readLogDirs struct {
	admin logDirsAdmin
}

// NewReadLogDirs adapts DescribeLogDirs (v4+ reports volume sizes) to the metrics.ReadLogDirs port.
func NewReadLogDirs(admin logDirsAdmin) metrics.ReadLogDirs {
	return readLogDirs{admin: admin}
}

func (r readLogDirs) Execute(ctx context.Context) ([]metrics.LogDir, error) {
	described, err := r.admin.DescribeAllLogDirs(ctx, nil)
	var dirs []metrics.LogDir
	described.Each(func(d kadm.DescribedLogDir) {
		dir := metrics.LogDir{Broker: d.Broker, Dir: d.Dir, TopicBytes: map[string]int64{}}
		if d.TotalBytes >= 0 {
			dir.TotalBytes = &d.TotalBytes
		}
		if d.UsableBytes >= 0 {
			dir.UsableBytes = &d.UsableBytes
		}
		if d.Err != nil {
			dir.Error = d.Err.Error()
		}
		d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			dir.KafkaBytes += p.Size
			dir.TopicBytes[p.Topic] += p.Size
		})
		dirs = append(dirs, dir)
	})
	if err != nil {
		return dirs, fmt.Errorf("describe log directories: %w", err)
	}
	return dirs, nil
}
