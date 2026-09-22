package fake

import (
	"context"
	"hash/fnv"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Produce appends records to topic's model, stamping f.now() when a record's
// Timestamp is zero (FUNC-SPEC §8.7 M5; TECH-SPEC §4.3). A record with an
// explicit, out-of-range Partition reports that record's own Err; the rest
// of the batch is unaffected.
func (f *Fake) Produce(ctx context.Context, topic string, records []kafka.ProduceRequest) ([]kafka.ProduceResult, error) {
	return invoke(f, ctx, "Produce", true, func() ([]kafka.ProduceResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		t := f.model.topics[topic]
		if t == nil {
			return nil, &kafka.Error{Kind: kafka.KindNotFound, Resource: "topic"}
		}

		out := make([]kafka.ProduceResult, len(records))
		for i, r := range records {
			partition := resolveProducePartition(t, r.Partition, r.Key)
			if partition < 0 || int(partition) >= len(t.partitions) {
				out[i] = kafka.ProduceResult{Err: &kafka.Error{Kind: kafka.KindNotFound, Resource: "partition"}}
				continue
			}

			ts := r.Timestamp
			if ts.IsZero() {
				ts = f.now()
			}
			part := &t.partitions[partition]
			offset := part.beginOffset + int64(len(part.records))
			part.records = append(part.records, kafka.Record{
				Topic: topic, Partition: partition, Offset: offset, Timestamp: ts,
				Key: r.Key, Value: r.Value, Headers: r.Headers,
			})
			out[i] = kafka.ProduceResult{Partition: partition, Offset: offset, Timestamp: ts}
		}
		return out, nil
	})
}

// resolveProducePartition honours an explicit partition when given, else
// hashes the key (FNV-1a) across t's partitions, or defaults to partition 0
// for a keyless record. Callers must hold f.mu.
func resolveProducePartition(t *fakeTopic, explicit *int32, key []byte) int32 {
	if explicit != nil {
		return *explicit
	}
	if len(t.partitions) == 0 || len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int32(h.Sum32() % uint32(len(t.partitions)))
}

// compile-time proof that Fake satisfies the Producer surface.
var _ kafka.Producer = (*Fake)(nil)
