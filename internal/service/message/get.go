package message

import (
	"context"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Get fetches one message by partition and offset (FUNC-SPEC §8.7 M2).
// Assigning at offset and reading whatever comes back covers both out-of-
// range (nothing arrives) and compacted-away (the first record's offset is
// greater than requested) — both report NotFound (FUNC-SPEC §9.1 rules C10).
// Under the data-plane lock (FUNC-SPEC §9.5), only an operator caller
// presenting a break-glass reason passes checkGate — which then reports a
// single HIGH RESULT audit event on success.
func (s *Service) Get(ctx context.Context, caller core.Caller, topic string, partition int32, offset int64) (scan.Record, error) {
	if err := s.checkGate(ctx, caller, "M2", topic); err != nil {
		return scan.Record{}, err
	}

	consumer, err := s.newConsumer()
	if err != nil {
		return scan.Record{}, err
	}
	defer consumer.Close()

	if err := consumer.Assign(ctx, topic, []int32{partition}, map[int32]int64{partition: offset}); err != nil {
		return scan.Record{}, err
	}
	records, err := consumer.Poll(ctx)
	if err != nil {
		return scan.Record{}, err
	}
	if len(records) == 0 || records[0].Offset > offset {
		return scan.Record{}, &kafka.Error{Kind: kafka.KindNotFound, Resource: "message"}
	}
	s.auditBreakGlassRead(ctx, caller, "M2", topic)
	return scan.Decode(records[0], ""), nil
}
