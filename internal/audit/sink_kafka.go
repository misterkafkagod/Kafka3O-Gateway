package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// KafkaSink produces each Event as one record to topic, via a
// kafka.Producer the composition root builds as a dedicated client separate
// from the data-plane producer (TECH-SPEC C5). Write is bounded by ctx —
// callers pass a context with whatever deadline the request pipeline needs
// (TECH-SPEC L5) — and returns any failure rather than blocking or
// retrying, so the caller can fail closed (FUNC-SPEC §8.5 V2).
type KafkaSink struct {
	producer kafka.Producer
	topic    string
}

// NewKafkaSink builds a KafkaSink producing to topic via producer.
func NewKafkaSink(producer kafka.Producer, topic string) *KafkaSink {
	return &KafkaSink{producer: producer, topic: topic}
}

// Write produces ev to the audit topic.
func (s *KafkaSink) Write(ctx context.Context, ev Event) error {
	value, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("audit: marshal event: %w", err)
	}

	results, err := s.producer.Produce(ctx, s.topic, []kafka.ProduceRequest{{Value: value}})
	if err != nil {
		return fmt.Errorf("audit: produce: %w", err)
	}
	if len(results) > 0 && results[0].Err != nil {
		return fmt.Errorf("audit: produce: %w", results[0].Err)
	}
	return nil
}

// compile-time proof that KafkaSink satisfies Sink.
var _ Sink = (*KafkaSink)(nil)
