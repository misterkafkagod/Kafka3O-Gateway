package message

import (
	"net/http"
	"strconv"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/scan"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

// toRecordDTOs converts decoded scan.Record values into the wire shape.
func toRecordDTOs(records []scan.Record) []RecordDTO {
	out := make([]RecordDTO, len(records))
	for i, r := range records {
		headers := make([]HeaderDTO, len(r.Headers))
		for j, h := range r.Headers {
			headers[j] = HeaderDTO{Key: h.Key, Value: h.Value, ValueEncoding: h.ValueEncoding}
		}
		out[i] = RecordDTO{
			Topic:         r.Topic,
			Partition:     r.Partition,
			Offset:        r.Offset,
			Timestamp:     r.Timestamp.UTC().Format(time.RFC3339Nano),
			TimestampMs:   r.Timestamp.UnixMilli(),
			Key:           r.Key,
			KeyEncoding:   r.KeyEncoding,
			Value:         r.Value,
			ValueEncoding: r.ValueEncoding,
			Headers:       headers,
			SizeBytes:     r.SizeBytes,
		}
	}
	return out
}

// toProduceResponseBody converts the service's message.ProduceResult into
// the wire bulk envelope (FUNC-SPEC §8.3 Bulk), and reports the HTTP status
// to use: 200 when every item succeeded, 207 when the outcome is mixed.
func toProduceResponseBody(r message.ProduceResult) (ProduceResponseBody, int) {
	items := make([]ProduceItemResultDTO, len(r.Items))
	for i, item := range r.Items {
		status := "ok"
		if item.Outcome != audit.OutcomeSucceeded {
			status = "failed"
		}
		items[i] = ProduceItemResultDTO{
			Index: item.Index, Status: status,
			Partition: item.Partition, Offset: item.Offset, TimestampMs: item.TimestampMs,
			Error: item.Error,
		}
	}
	status := http.StatusOK
	if r.Summary.Failed > 0 {
		status = http.StatusMultiStatus
	}
	return ProduceResponseBody{
		Items: items,
		Summary: BulkSummaryDTO{
			Total: r.Summary.Total, OK: r.Summary.Succeeded, Failed: r.Summary.Failed,
		},
	}, status
}

// toScanStatsDTO converts the service's scan.Stats into the wire shape,
// nulling StoppedBy when the scan reached its end snapshot (FUNC-SPEC §8.3).
func toScanStatsDTO(s scan.Stats) ScanStatsDTO {
	var stoppedBy *string
	if s.StoppedBy != "" {
		v := s.StoppedBy
		stoppedBy = &v
	}
	continuation := make(map[string]int64, len(s.Continuation))
	for p, offset := range s.Continuation {
		continuation[strconv.Itoa(int(p))] = offset
	}
	return ScanStatsDTO{
		Scanned:      s.Scanned,
		Matched:      s.Matched,
		Skipped:      s.Skipped,
		Bytes:        s.Bytes,
		ElapsedMs:    s.ElapsedMs,
		ReachedEnd:   s.ReachedEnd,
		StoppedBy:    stoppedBy,
		Continuation: continuation,
	}
}
