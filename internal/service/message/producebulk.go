package message

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// ProduceBulk parses body — NDJSON (one JSON record per line) when ndjson is
// true, else a JSON array — into records and produces them exactly as
// Produce does (FUNC-SPEC §8.7 M6, M5). body over
// s.bounds.MaxBulkBodyBytes reports *core.PolicyError{Code:
// core.PayloadTooLarge} before any parsing runs (FUNC-SPEC §8.8); a
// malformed body reports *core.PolicyError{Code: core.Validation} — neither
// case executes anything or emits an ATTEMPT.
func (s *Service) ProduceBulk(ctx context.Context, caller core.Caller, topic string, body io.Reader, ndjson bool) (ProduceResult, error) {
	if err := s.checkGate(ctx, caller, "M6", topic); err != nil {
		return ProduceResult{}, err
	}
	items, err := s.parseBulkBody(body, ndjson)
	if err != nil {
		return ProduceResult{}, err
	}
	return s.produce(ctx, caller, "M6", topic, items)
}

// parseBulkBody enforces the body-size ceiling, then decodes it.
func (s *Service) parseBulkBody(body io.Reader, ndjson bool) ([]ProduceItem, error) {
	limit := s.bounds.MaxBulkBodyBytes
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("message: read bulk body: %w", err)
	}
	if int64(len(data)) > limit {
		return nil, &core.PolicyError{
			Code:    core.PayloadTooLarge,
			Message: fmt.Sprintf("request body exceeds the %d byte limit", limit),
		}
	}
	if ndjson {
		return parseNDJSON(data)
	}
	return parseJSONArray(data)
}

// parseNDJSON decodes data as one JSON ProduceItem per non-blank line.
func parseNDJSON(data []byte) ([]ProduceItem, error) {
	var items []ProduceItem
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), len(data)+1)

	line := 0
	for scanner.Scan() {
		line++
		text := bytes.TrimSpace(scanner.Bytes())
		if len(text) == 0 {
			continue
		}
		var item ProduceItem
		if err := json.Unmarshal(text, &item); err != nil {
			return nil, &core.PolicyError{
				Code:    core.Validation,
				Message: fmt.Sprintf("line %d: invalid JSON: %s", line, err),
			}
		}
		items = append(items, item)
	}
	if err := scanner.Err(); err != nil {
		return nil, &core.PolicyError{Code: core.Validation, Message: "invalid NDJSON body: " + err.Error()}
	}
	return items, nil
}

// parseJSONArray decodes data as a single JSON array of ProduceItem.
func parseJSONArray(data []byte) ([]ProduceItem, error) {
	var items []ProduceItem
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, &core.PolicyError{Code: core.Validation, Message: "invalid JSON array body: " + err.Error()}
	}
	return items, nil
}
