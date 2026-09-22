package message

import (
	"context"
	"fmt"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// ProduceHeader is one header on a record to produce (FUNC-SPEC §8.7 M5).
// It carries JSON tags: unlike internal/kafka's domain types (TECH-SPEC I5),
// this struct IS the wire shape for M5's body and M6's NDJSON/array records
// — the API layer (Task 5.5) decodes directly into it, and into ProduceItem
// below, rather than duplicating an identical DTO.
type ProduceHeader struct {
	Key           string `json:"key"`
	Value         string `json:"value"`
	ValueEncoding string `json:"valueEncoding,omitempty"`
}

// ProduceItem is one record to produce, before validation (FUNC-SPEC §8.7 M5).
type ProduceItem struct {
	Key           string          `json:"key,omitempty"`
	KeyEncoding   string          `json:"keyEncoding,omitempty"`
	Value         string          `json:"value"`
	ValueEncoding string          `json:"valueEncoding,omitempty"`
	Headers       []ProduceHeader `json:"headers,omitempty"`
	Partition     *int32          `json:"partition,omitempty"`
	TimestampMs   *int64          `json:"timestampMs,omitempty"`
}

// ProduceItemResult is one item's outcome (FUNC-SPEC §8.7 M5).
type ProduceItemResult struct {
	Index       int
	Outcome     audit.Outcome
	Partition   int32
	Offset      int64
	TimestampMs int64
	Error       string
}

// ProduceResult is Produce's (and ProduceBulk's) output (FUNC-SPEC §8.7 M5, M6).
type ProduceResult struct {
	Items   []ProduceItemResult
	Summary core.BulkSummary
}

// Produce validates and writes items to topic (FUNC-SPEC §8.7 M5): any
// invalid item (bad encoding, out-of-range partition) reports
// *core.PolicyError{Code: core.BulkValidationFailed} naming every failing
// item and produces nothing, no ATTEMPT audit. Every valid item then
// produces independently (FUNC-SPEC §9.4): one item's broker-side failure
// never stops the others, and the ATTEMPT/RESULT audit pair brackets
// execution regardless of the per-item mix.
func (s *Service) Produce(ctx context.Context, caller core.Caller, topic string, items []ProduceItem) (ProduceResult, error) {
	if err := s.checkGate(ctx, caller, "M5", topic); err != nil {
		return ProduceResult{}, err
	}
	return s.produce(ctx, caller, "M5", topic, items)
}

// produce is Produce and ProduceBulk's shared implementation, run only after
// the caller has already passed checkGate — the two commands differ only in
// how their items arrive (a body object/array for M5, an uploaded NDJSON/
// JSON-array stream for M6 — see producebulk.go).
func (s *Service) produce(ctx context.Context, caller core.Caller, commandID, topic string, items []ProduceItem) (ProduceResult, error) {
	partitionCount, err := s.partitionCount(ctx, topic)
	if err != nil {
		return ProduceResult{}, err
	}

	// produced captures each item's actual kafka.ProduceResult (partition,
	// offset, timestamp) alongside BulkExecute's own per-item outcome/error
	// (core.BulkItemResult carries neither — it is produce-agnostic): next,
	// an index counter closed over by execute, safe because BulkExecute
	// calls execute exactly once per item, strictly in index order.
	produced := make([]kafka.ProduceResult, len(items))
	next := 0
	attempt := s.newEvent(caller, commandID, topic)
	br, err := core.BulkRun(ctx, s.auditor, attempt,
		func(summary core.BulkSummary) audit.Event { return s.resultEvent(attempt, summary) },
		items,
		func(item ProduceItem) error { return validateProduceItem(item, partitionCount) },
		func(ctx context.Context, item ProduceItem) error {
			idx := next
			next++
			results, err := s.producer.Produce(ctx, topic, []kafka.ProduceRequest{toProduceRequest(item)})
			if err != nil {
				return err
			}
			produced[idx] = results[0]
			return results[0].Err
		},
	)
	if err != nil {
		return ProduceResult{}, err
	}
	return toProduceResult(produced, br), nil
}

// partitionCount returns topic's partition count, for validating an
// explicit Partition against.
func (s *Service) partitionCount(ctx context.Context, topic string) (int32, error) {
	t, err := s.admin.DescribeTopics(ctx, topic)
	if err != nil {
		return 0, err
	}
	return int32(len(t.Partitions)), nil
}

// validateProduceItem checks one item's encodings and, when given, its
// explicit partition (FUNC-SPEC §8.7 M5).
func validateProduceItem(item ProduceItem, partitionCount int32) error {
	if _, err := scan.EncodeValue(item.Key, item.KeyEncoding); err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}
	if _, err := scan.EncodeValue(item.Value, item.ValueEncoding); err != nil {
		return fmt.Errorf("invalid value: %w", err)
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

// toProduceRequest converts an already-validated ProduceItem into a
// kafka.ProduceRequest; EncodeValue errors are ignored here because
// validateProduceItem already proved every value encodes cleanly.
func toProduceRequest(item ProduceItem) kafka.ProduceRequest {
	key, _ := scan.EncodeValue(item.Key, item.KeyEncoding)
	value, _ := scan.EncodeValue(item.Value, item.ValueEncoding)
	headers := make([]kafka.Header, len(item.Headers))
	for i, h := range item.Headers {
		v, _ := scan.EncodeValue(h.Value, h.ValueEncoding)
		headers[i] = kafka.Header{Key: h.Key, Value: v}
	}
	req := kafka.ProduceRequest{Key: key, Value: value, Headers: headers, Partition: item.Partition}
	if item.TimestampMs != nil {
		req.Timestamp = time.UnixMilli(*item.TimestampMs)
	}
	return req
}

// toProduceResult pairs BulkResult's per-item outcome/error with the actual
// kafka.ProduceResult produce captured for each item, in the same index
// order.
func toProduceResult(produced []kafka.ProduceResult, br core.BulkResult) ProduceResult {
	out := make([]ProduceItemResult, len(br.Items))
	for i, r := range br.Items {
		out[i] = ProduceItemResult{Index: r.Index, Outcome: r.Outcome, Error: r.Error}
		if r.Outcome != audit.OutcomeSucceeded {
			continue
		}
		out[i].Partition = produced[i].Partition
		out[i].Offset = produced[i].Offset
		out[i].TimestampMs = produced[i].Timestamp.UnixMilli()
	}
	return ProduceResult{Items: out, Summary: br.Summary}
}
