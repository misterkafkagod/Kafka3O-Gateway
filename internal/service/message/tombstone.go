package message

import (
	"context"
	"fmt"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// TombstoneItem is M7's input (FUNC-SPEC §8.7 M7): unlike ProduceItem, it
// carries no value — a tombstone's record value is always nil, Kafka's
// convention for "delete this key" under log compaction.
type TombstoneItem struct {
	Key         string          `json:"key"`
	KeyEncoding string          `json:"keyEncoding,omitempty"`
	Partition   *int32          `json:"partition,omitempty"`
	Headers     []ProduceHeader `json:"headers,omitempty"`
}

// TombstoneResult is M7's output (FUNC-SPEC §8.7 M7).
type TombstoneResult struct {
	Partition int32
	Offset    int64
}

// Tombstone validates and writes a nil-value record for item.Key (FUNC-SPEC
// §8.7 M7), bracketed by the same ATTEMPT/RESULT audit pair as Produce —
// unlike M5/M6, M7 is always exactly one record, so it sequences the two
// phases directly rather than through core.BulkRun.
func (s *Service) Tombstone(ctx context.Context, caller core.Caller, topic string, item TombstoneItem) (TombstoneResult, error) {
	partitionCount, err := s.partitionCount(ctx, topic)
	if err != nil {
		return TombstoneResult{}, err
	}
	if err := validateTombstoneItem(item, partitionCount); err != nil {
		return TombstoneResult{}, &core.PolicyError{Code: core.Validation, Message: err.Error()}
	}

	attempt := s.newEvent(caller, "M7", topic)
	if err := s.auditor.Attempt(ctx, attempt); err != nil {
		return TombstoneResult{}, &core.PolicyError{Code: core.AuditUnavailable, Message: err.Error()}
	}

	result, err := s.produceTombstone(ctx, topic, item)
	if err != nil {
		s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeFailed, errCodeOf(err)))
		return TombstoneResult{}, err
	}
	s.auditor.Result(ctx, s.resultEventOutcome(attempt, audit.OutcomeSucceeded, ""))
	return result, nil
}

// produceTombstone builds and sends item's tombstone record.
func (s *Service) produceTombstone(ctx context.Context, topic string, item TombstoneItem) (TombstoneResult, error) {
	key, _ := scan.EncodeValue(item.Key, item.KeyEncoding)
	headers := make([]kafka.Header, len(item.Headers))
	for i, h := range item.Headers {
		v, _ := scan.EncodeValue(h.Value, h.ValueEncoding)
		headers[i] = kafka.Header{Key: h.Key, Value: v}
	}
	req := kafka.ProduceRequest{Key: key, Value: nil, Headers: headers, Partition: item.Partition}

	produced, err := s.producer.Produce(ctx, topic, []kafka.ProduceRequest{req})
	if err != nil {
		return TombstoneResult{}, err
	}
	if produced[0].Err != nil {
		return TombstoneResult{}, produced[0].Err
	}
	return TombstoneResult{Partition: produced[0].Partition, Offset: produced[0].Offset}, nil
}

// validateTombstoneItem checks item's key encoding, header encodings, and,
// when given, its explicit partition (FUNC-SPEC §8.7 M7).
func validateTombstoneItem(item TombstoneItem, partitionCount int32) error {
	if _, err := scan.EncodeValue(item.Key, item.KeyEncoding); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	for _, h := range item.Headers {
		if _, err := scan.EncodeValue(h.Value, h.ValueEncoding); err != nil {
			return fmt.Errorf("invalid header %q: %w", h.Key, err)
		}
	}
	if item.Partition != nil && (*item.Partition < 0 || *item.Partition >= partitionCount) {
		return fmt.Errorf("partition %d out of range [0,%d)", *item.Partition, partitionCount)
	}
	return nil
}
