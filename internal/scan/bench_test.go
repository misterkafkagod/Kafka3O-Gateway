package scan_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
	"github.com/misterkafkagod/kafka3o/internal/scan"
)

// BenchmarkScanRun measures Run's cost over a single-partition, fully
// exhausted scan of n records (TECH-SPEC §4.7). BenchmarkEncodeScanEnvelope
// is deferred to Task 3.4, which owns the envelope type this benchmark would
// encode.
func BenchmarkScanRun(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("%dkrecords", n/1000), func(b *testing.B) {
			records := make([]kafka.Record, n)
			for i := range records {
				records[i] = kafka.Record{Partition: 0, Value: []byte(fmt.Sprintf(`{"i":%d}`, i))}
			}
			f := fake.New()
			f.SeedTopic("t", 1, records...)
			spec := scan.Spec{Topic: "t", Partitions: map[int32]scan.PartitionSpec{0: {Start: 0, End: int64(n)}}}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := scan.Run(context.Background(), f, spec, scan.MatchAll, func(scan.Record) {}, time.Now); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecode measures Decode's cost for each auto-detected encoding
// (TECH-SPEC §4.7).
func BenchmarkDecode(b *testing.B) {
	cases := []struct {
		name string
		r    kafka.Record
	}{
		{"json", kafka.Record{Value: []byte(`{"a":1,"b":"text","c":[1,2,3]}`)}},
		{"string", kafka.Record{Value: []byte("a plain utf-8 text value for benchmarking")}},
		{"binary", kafka.Record{Value: []byte{0xff, 0xfe, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05}}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				scan.Decode(tc.r, "")
			}
		})
	}
}

// BenchmarkRegexMatch measures NewRegexMatcher's per-record cost (TECH-SPEC §4.7).
func BenchmarkRegexMatch(b *testing.B) {
	m, err := scan.NewRegexMatcher("FAILED", nil, false, 0)
	if err != nil {
		b.Fatal(err)
	}
	record := scan.Record{Value: "2026-01-01T00:00:00Z INFO something happened, not FAILED here"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m(record)
	}
}

// BenchmarkJSONPathEval measures NewJSONPathMatcher's per-record cost,
// including the JSON unmarshal it does on every call (TECH-SPEC §4.7).
func BenchmarkJSONPathEval(b *testing.B) {
	m, err := scan.NewJSONPathMatcher("$.status", scan.OpEq, "FAILED")
	if err != nil {
		b.Fatal(err)
	}
	record := scan.Record{Value: `{"status":"FAILED","code":500,"tags":["a","b","c"]}`}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m(record)
	}
}
