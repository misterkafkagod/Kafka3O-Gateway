package message_test

import (
	"encoding/json"
	"testing"

	apimessage "github.com/misterkafkagod/kafka3o/internal/api/message"
)

// BenchmarkEncodeScanEnvelope measures json.Marshal's cost for a 100-item
// scan envelope (TECH-SPEC §4.7) — deferred here from Task 3.2, whose
// internal/scan package owns no wire-format DTO to encode.
func BenchmarkEncodeScanEnvelope(b *testing.B) {
	items := make([]apimessage.RecordDTO, 100)
	for i := range items {
		items[i] = apimessage.RecordDTO{
			Topic: "t", Partition: 0, Offset: int64(i),
			Timestamp: "2026-01-01T00:00:00Z", TimestampMs: 1767225600000,
			Key: "", KeyEncoding: "string",
			Value: `{"i":` + string(rune('0'+i%10)) + `}`, ValueEncoding: "json",
			Headers: nil, SizeBytes: 10,
		}
	}
	body := apimessage.ReadMessagesBody{
		Items: items,
		Scan: apimessage.ScanStatsDTO{
			Scanned: 100, Matched: 100, Bytes: 1000, ElapsedMs: 5,
			ReachedEnd: true, Continuation: map[string]int64{"0": 100},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(body); err != nil {
			b.Fatal(err)
		}
	}
}
