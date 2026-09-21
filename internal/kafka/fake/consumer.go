package fake

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// consumerSession is the fake's single active manual-assignment session.
type consumerSession struct {
	topic  string
	cursor map[int32]int64 // next offset to read, per assigned partition
}

// Assign begins a manual-assignment session over topic's partitions, each
// starting at startOffsets[partition] (FUNC-SPEC §9.2 Assign).
func (f *Fake) Assign(ctx context.Context, topic string, partitions []int32, startOffsets map[int32]int64) error {
	_, err := invoke(f, ctx, "Assign", false, func() (struct{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		cursor := make(map[int32]int64, len(partitions))
		for _, p := range partitions {
			cursor[p] = startOffsets[p]
		}
		f.consumerSession = &consumerSession{topic: topic, cursor: cursor}
		return struct{}{}, nil
	})
	return err
}

// Poll returns every record from the current cursor to the end of each
// assigned partition's log, then advances the cursor past them (FUNC-SPEC
// §9.2 Poll). Reading beyond the log's end returns no records and no error,
// the same as Assign never having been called.
func (f *Fake) Poll(ctx context.Context) ([]kafka.Record, error) {
	return invoke(f, ctx, "Poll", false, func() ([]kafka.Record, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		session := f.consumerSession
		if session == nil {
			return nil, nil
		}
		t := f.model.topics[session.topic]
		if t == nil {
			return nil, nil
		}

		var out []kafka.Record
		for p, cursor := range session.cursor {
			if p < 0 || int(p) >= len(t.partitions) {
				continue
			}
			part := t.partitions[p]
			start := cursor - part.beginOffset
			if start < 0 {
				start = 0
			}
			if start >= int64(len(part.records)) {
				continue
			}
			out = append(out, part.records[start:]...)
			session.cursor[p] = part.beginOffset + int64(len(part.records))
		}
		return out, nil
	})
}

// Close ends the session (TECH-SPEC §2.3). It never fails and is not
// fault-injectable — closing a client is not itself a Kafka operation.
func (f *Fake) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumerSession = nil
}

// compile-time proof that Fake satisfies the Consumer surface.
var _ kafka.Consumer = (*Fake)(nil)
