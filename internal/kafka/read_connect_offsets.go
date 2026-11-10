package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

// connectOffsetsReadLimit bounds one read, so a partition whose end is never reached cannot stall the backup list.
const connectOffsetsReadLimit = 5 * time.Second

type endOffsetsLister interface {
	ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
}

type readConnectOffsets struct {
	admin  endOffsetsLister
	client *kgo.Client
	topic  string
}

// NewReadConnectOffsets reads Kafka Connect's offsets topic, which holds each S3 source object's committed record count.
func NewReadConnectOffsets(admin endOffsetsLister, client *kgo.Client, topic string) backup.ReadRestoreOffsets {
	return readConnectOffsets{admin: admin, client: client, topic: topic}
}

// Execute reads the topic from its start to its current end and keeps the latest count per object.
func (r readConnectOffsets) Execute(ctx context.Context, connector string) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, connectOffsetsReadLimit)
	defer cancel()
	starts, err := r.admin.ListStartOffsets(ctx, r.topic)
	if err != nil {
		return nil, fmt.Errorf("list %s start offsets: %w", r.topic, err)
	}
	ends, err := r.admin.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return nil, fmt.Errorf("list %s end offsets: %w", r.topic, err)
	}
	pending := map[int32]int64{}
	consume := map[int32]kgo.Offset{}
	for partition, end := range ends[r.topic] {
		if end.Err != nil {
			return nil, fmt.Errorf("%s partition %d: %w", r.topic, partition, end.Err)
		}
		start := starts[r.topic][partition].Offset
		if end.Offset > start {
			pending[partition] = end.Offset
			consume[partition] = kgo.NewOffset().At(start)
		}
	}
	counts := map[string]int64{}
	if len(pending) == 0 {
		return counts, nil
	}
	opts := append([]kgo.Opt(nil), r.client.Opts()...)
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{r.topic: consume}))
	reader, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create offsets reader: %w", err)
	}
	defer reader.Close()
	for len(pending) > 0 {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read %s: %w", r.topic, err)
		}
		for _, fetchErr := range fetches.Errors() {
			return nil, fmt.Errorf("read %s: %w", r.topic, fetchErr.Err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			ApplyConnectOffsetRecord(counts, connector, record.Key, record.Value)
			if end, ok := pending[record.Partition]; ok && record.Offset+1 >= end {
				delete(pending, record.Partition)
			}
		})
	}
	return counts, nil
}
