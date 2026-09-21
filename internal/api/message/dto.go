// Package message wires the message-reading commands (FUNC-SPEC §8.7 M1,
// M2) onto Huma operations: routes, DTOs, the `from=`/`to=` wire parser, and
// the mapping between them and internal/service/message's plain domain
// results (TECH-SPEC I5).
package message

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
