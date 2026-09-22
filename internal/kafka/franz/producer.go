package franz

import (
	"context"
	"hash/fnv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// unsetPartition marks a kgo.Record whose ProduceRequest left Partition nil
// (explicitOrDefaultPartitioner then hashes the key, or round-robins).
const unsetPartition = -1

// explicitOrDefaultPartitioner honours a record's explicit partition
// (TECH-SPEC §2.3, C4) when Client.Produce set one, and otherwise falls
// back to a key hash (FNV-1a) or, for a keyless record, round-robin — kgo's
// own default partitioners are unusable here because a client-wide
// Partitioner cannot see which individual records asked for a specific
// partition and which did not.
type explicitOrDefaultPartitioner struct{}

func (explicitOrDefaultPartitioner) ForTopic(string) kgo.TopicPartitioner {
	return &explicitOrDefaultTopicPartitioner{}
}

type explicitOrDefaultTopicPartitioner struct {
	next atomic.Int64
}

func (*explicitOrDefaultTopicPartitioner) RequiresConsistency(*kgo.Record) bool { return true }

func (p *explicitOrDefaultTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition != unsetPartition {
		// Returned verbatim, even out of range: the broker rejects it and
		// that rejection becomes this record's own ProduceResult.Err
		// (Task 5.1 "Producer_PartitionOutOfRangeIsError").
		return int(r.Partition)
	}
	if len(r.Key) == 0 {
		return int(p.next.Add(1)-1) % n
	}
	h := fnv.New32a()
	_, _ = h.Write(r.Key)
	return int(h.Sum32()) % n
}

// Produce writes records to topic using the shared client (TECH-SPEC §2.3,
// C4: acks=all, idempotent, no dedicated per-call client). Produce blocks
// until every record has a result (FUNC-SPEC §8.7 M5).
func (c *Client) Produce(ctx context.Context, topic string, records []kafka.ProduceRequest) ([]kafka.ProduceResult, error) {
	krecords := make([]*kgo.Record, len(records))
	for i, r := range records {
		kr := &kgo.Record{
			Topic:     topic,
			Key:       r.Key,
			Value:     r.Value,
			Partition: unsetPartition,
		}
		if r.Partition != nil {
			kr.Partition = *r.Partition
		}
		if !r.Timestamp.IsZero() {
			kr.Timestamp = r.Timestamp
		}
		kr.Headers = make([]kgo.RecordHeader, len(r.Headers))
		for j, h := range r.Headers {
			kr.Headers[j] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
		}
		krecords[i] = kr
	}

	results := c.kgo.ProduceSync(ctx, krecords...)

	out := make([]kafka.ProduceResult, len(results))
	for i, res := range results {
		if res.Err != nil {
			out[i] = kafka.ProduceResult{Err: wrapErr("partition", res.Err)}
			continue
		}
		ts := res.Record.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		out[i] = kafka.ProduceResult{Partition: res.Record.Partition, Offset: res.Record.Offset, Timestamp: ts}
	}
	return out, nil
}

// compile-time proof that Client satisfies kafka.Producer.
var _ kafka.Producer = (*Client)(nil)
