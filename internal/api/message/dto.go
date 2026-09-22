// Package message wires the message commands (FUNC-SPEC §8.7 M1-M7) onto
// Huma operations: routes, DTOs, the `from=`/`to=` wire parser, and the
// mapping between them and internal/service/message's plain domain results
// (TECH-SPEC I5).
package message

import (
	"encoding/json"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// HeaderDTO is one decoded record header (FUNC-SPEC §8.3 Record row).
type HeaderDTO struct {
	Key           string `json:"key"`
	Value         string `json:"value"`
	ValueEncoding string `json:"valueEncoding"`
}

// RecordDTO is one message (FUNC-SPEC §8.3 Record row): Timestamp is
// ISO-8601, TimestampMs the same instant in epoch milliseconds.
type RecordDTO struct {
	Topic         string      `json:"topic"`
	Partition     int32       `json:"partition"`
	Offset        int64       `json:"offset"`
	Timestamp     string      `json:"timestamp"`
	TimestampMs   int64       `json:"timestampMs"`
	Key           string      `json:"key"`
	KeyEncoding   string      `json:"keyEncoding"`
	Value         string      `json:"value"`
	ValueEncoding string      `json:"valueEncoding"`
	Headers       []HeaderDTO `json:"headers"`
	SizeBytes     int         `json:"sizeBytes"`
}

// ScanStatsDTO is the scan envelope's `scan` sub-object (FUNC-SPEC §8.3).
// StoppedBy is nil (JSON null) when the scan reached its end snapshot.
type ScanStatsDTO struct {
	Scanned      int              `json:"scanned"`
	Matched      int              `json:"matched"`
	Skipped      int              `json:"skipped"`
	Bytes        int64            `json:"bytes"`
	ElapsedMs    int64            `json:"elapsedMs"`
	ReachedEnd   bool             `json:"reachedEnd"`
	StoppedBy    *string          `json:"stoppedBy"`
	Continuation map[string]int64 `json:"continuation"`
}

// ReadMessagesInput is GET .../messages's parameters (FUNC-SPEC §8.7 M1;
// TECH-SPEC §6.2). Partition omitted means every partition. Limit, MaxBytes,
// and MaxTimeMs have no static maximum tag: their ceilings are configured
// (FUNC-SPEC §8.8), enforced in internal/service/message, not the schema.
type ReadMessagesInput struct {
	Name string `path:"name"`
	// Partition is -1 (its default) when omitted — every partition — since
	// Huma v2 does not support pointers for query parameters.
	Partition int32  `query:"partition" default:"-1"`
	From      string `query:"from" required:"true"`
	To        string `query:"to"`
	Limit     int    `query:"limit"`
	MaxBytes  int64  `query:"maxBytes"`
	MaxTimeMs int64  `query:"maxTimeMs"`
	Format    string `query:"format"`
}

// ReadMessagesBody is GET .../messages's response body (FUNC-SPEC §8.3 scan envelope).
type ReadMessagesBody struct {
	Items []RecordDTO  `json:"items"`
	Scan  ScanStatsDTO `json:"scan"`
}

// ReadMessagesOutput wraps ReadMessagesBody for Huma.
type ReadMessagesOutput struct {
	Body ReadMessagesBody
}

// GetMessageInput identifies the message to fetch (FUNC-SPEC §8.7 M2).
type GetMessageInput struct {
	Name      string `path:"name"`
	Partition int32  `path:"partition"`
	Offset    int64  `path:"offset"`
}

// GetMessageOutput wraps a RecordDTO for Huma.
type GetMessageOutput struct {
	Body RecordDTO
}

// SearchBody is POST .../messages/search's request body (FUNC-SPEC §8.7 M3).
// Limit is present only so its presence can be rejected with
// VALIDATION_FAILED (FUNC-SPEC M3/M4 "limit is not accepted"; TECH-SPEC B8).
type SearchBody struct {
	Partition       *int32   `json:"partition,omitempty"`
	From            string   `json:"from"`
	To              string   `json:"to,omitempty"`
	Regex           string   `json:"regex"`
	Fields          []string `json:"fields,omitempty"`
	CaseInsensitive bool     `json:"caseInsensitive,omitempty"`
	MaxScan         int      `json:"maxScan,omitempty"`
	MaxMatches      int      `json:"maxMatches,omitempty"`
	MaxBytes        int64    `json:"maxBytes,omitempty"`
	MaxTimeMs       int64    `json:"maxTimeMs,omitempty"`
	Format          string   `json:"format,omitempty"`
	Limit           *int     `json:"limit,omitempty"`
}

// SearchInput is POST .../messages/search's parameters.
type SearchInput struct {
	Name string `path:"name"`
	Body SearchBody
}

// FilterCriteria is M4's nested `filter` object (FUNC-SPEC V4).
type FilterCriteria struct {
	Path  string `json:"path"`
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
}

// FilterBody is POST .../messages/filter's request body (FUNC-SPEC §8.7 M4).
// Limit is present only so its presence can be rejected with
// VALIDATION_FAILED (FUNC-SPEC M3/M4 "limit is not accepted"; TECH-SPEC B8).
type FilterBody struct {
	Partition  *int32         `json:"partition,omitempty"`
	From       string         `json:"from"`
	To         string         `json:"to,omitempty"`
	Filter     FilterCriteria `json:"filter"`
	MaxScan    int            `json:"maxScan,omitempty"`
	MaxMatches int            `json:"maxMatches,omitempty"`
	MaxBytes   int64          `json:"maxBytes,omitempty"`
	MaxTimeMs  int64          `json:"maxTimeMs,omitempty"`
	Format     string         `json:"format,omitempty"`
	Limit      *int           `json:"limit,omitempty"`
}

// FilterInput is POST .../messages/filter's parameters.
type FilterInput struct {
	Name string `path:"name"`
	Body FilterBody
}

// ProduceBody is M5's request body: a single record object, or
// { records: [...record] } (FUNC-SPEC §8.7 M5). message.ProduceItem already
// carries the wire tags for one record (internal/service/message, Task 5.3),
// so this type exists only to normalise whichever shape arrived into
// Records — Schema keeps Huma's pre-validation permissive (any object),
// since a static struct schema cannot express "either shape," and
// UnmarshalJSON does the real parsing.
type ProduceBody struct {
	Records []message.ProduceItem
}

// Schema implements huma.SchemaProvider.
func (ProduceBody) Schema(huma.Registry) *huma.Schema {
	return &huma.Schema{Type: huma.TypeObject}
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *ProduceBody) UnmarshalJSON(data []byte) error {
	var wrapped struct {
		Records []message.ProduceItem `json:"records"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && wrapped.Records != nil {
		b.Records = wrapped.Records
		return nil
	}
	var single message.ProduceItem
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	b.Records = []message.ProduceItem{single}
	return nil
}

// ProduceInput is POST .../messages's parameters (FUNC-SPEC §8.7 M5).
type ProduceInput struct {
	Name string `path:"name"`
	Body ProduceBody
}

// ProduceItemResultDTO is one produced (or failed) record (FUNC-SPEC §8.3
// Bulk, §8.7 M5): Partition, Offset, and TimestampMs are meaningful only
// when Status is "ok".
type ProduceItemResultDTO struct {
	Index       int    `json:"index"`
	Status      string `json:"status"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	TimestampMs int64  `json:"timestampMs"`
	Error       string `json:"error,omitempty"`
}

// BulkSummaryDTO is the bulk envelope's `summary` sub-object (FUNC-SPEC §8.3).
type BulkSummaryDTO struct {
	Total  int `json:"total"`
	OK     int `json:"ok"`
	Failed int `json:"failed"`
}

// ProduceResponseBody is M5 and M6's response body (FUNC-SPEC §8.3 Bulk).
type ProduceResponseBody struct {
	Items   []ProduceItemResultDTO `json:"items"`
	Summary BulkSummaryDTO         `json:"summary"`
}

// ProduceOutput wraps ProduceResponseBody for Huma. Status is Huma's
// dynamic-status-code field (matched by name, not tag): 200 when every item
// succeeded, 207 when the outcome is mixed (FUNC-SPEC §8.3 Bulk).
type ProduceOutput struct {
	Status int
	Body   ProduceResponseBody
}

// ProduceBulkInput is POST .../messages/bulk's parameters (FUNC-SPEC §8.7
// M6): RawBody is the whole, unparsed body — NDJSON or a JSON array — so
// Huma neither JSON-decodes nor schema-validates it (that would reject
// NDJSON outright, or reject a JSON array against a binary-string schema —
// Huma treats a "contentType:application/json" tag on RawBody as a signal
// to run its own JSON schema validation, which is exactly what must not
// happen here). The handler checks the real Content-Type header itself and
// accepts either application/json or application/x-ndjson (FUNC-SPEC §8.2);
// the OpenAPI doc this produces just says "octet-stream," Huma's generic
// raw-body type, since RawBody cannot document two content types at once.
type ProduceBulkInput struct {
	Name        string `path:"name"`
	ContentType string `header:"Content-Type"`
	RawBody     []byte
}

// TombstoneBody is POST .../tombstones's request body (FUNC-SPEC §8.7 M7).
// message.TombstoneItem already carries the wire tags.
type TombstoneInput struct {
	Name string `path:"name"`
	Body message.TombstoneItem
}

// TombstoneResponseBody is M7's response body (FUNC-SPEC §8.7 M7).
type TombstoneResponseBody struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// TombstoneOutput wraps TombstoneResponseBody for Huma.
type TombstoneOutput struct {
	Body TombstoneResponseBody
}

// ReplaySourceDTO is M8's `source` request field (FUNC-SPEC §8.7 M8). From
// and To reuse M1's own `from=`/`to=` vocabulary and parser (ParseFrom):
// beginning, latest, offset:<n>, timestamp:<ms|iso> — a resumed call passes
// the prior response's cursor value back as e.g. "offset:<cursor>".
type ReplaySourceDTO struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions,omitempty"`
	From       string  `json:"from"`
	To         string  `json:"to,omitempty"`
}

// ReplayTargetDTO is M8's `target` request field (FUNC-SPEC §8.7 M8).
type ReplayTargetDTO struct {
	Topic             string `json:"topic"`
	PreservePartition bool   `json:"preservePartition,omitempty"`
}

// ReplayRequestBody is POST /v1/replays's request body (FUNC-SPEC §8.7 M8).
type ReplayRequestBody struct {
	Confirm string          `json:"confirm"`
	Source  ReplaySourceDTO `json:"source"`
	Target  ReplayTargetDTO `json:"target"`
	Limit   int             `json:"limit,omitempty"`
}

// ReplayInput is POST /v1/replays's parameters (FUNC-SPEC §8.7 M8, §8.2
// `?dryRun=true`).
type ReplayInput struct {
	DryRun bool `query:"dryRun"`
	Body   ReplayRequestBody
}

// ReplayPlanDTO is a dry-run M8's plan (FUNC-SPEC §8.6).
type ReplayPlanDTO struct {
	EstimatedRecords int64 `json:"estimatedRecords"`
	SourcePartitions int   `json:"sourcePartitions"`
	TargetPartitions int   `json:"targetPartitions"`
}

// ReplayResponseBody is POST /v1/replays's response body: either the
// executed copy's outcome directly (FUNC-SPEC §8.7 M8), or — when DryRun —
// the FUNC-SPEC §8.3 dry-run envelope. See topic.CreateTopicBody for why
// both shapes share one Go type. Copied and ReachedEnd are pointers, not
// plain omitempty values, because 0 and false are themselves meaningful on
// a real response (nothing copied yet; the window is not yet exhausted) and
// must still be rendered, unlike their zero value meaning "absent" on a
// dry-run response.
type ReplayResponseBody struct {
	DryRun     bool             `json:"dryRun,omitempty"`
	Plan       *ReplayPlanDTO   `json:"plan,omitempty"`
	Copied     *int             `json:"copied,omitempty"`
	Cursor     map[string]int64 `json:"cursor,omitempty"`
	ReachedEnd *bool            `json:"reachedEnd,omitempty"`
}

// ReplayOutput wraps ReplayResponseBody for Huma.
type ReplayOutput struct {
	Body ReplayResponseBody
}
