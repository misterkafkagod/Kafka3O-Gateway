package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

type fakeGet struct {
	bodies map[string][]byte
	calls  int
}

func (f *fakeGet) Execute(_ context.Context, key string) (io.ReadCloser, error) {
	f.calls++
	body, ok := f.bodies[key]
	if !ok {
		return nil, s3store.ErrUnavailable
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func gzipped(text string) []byte {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, _ = writer.Write([]byte(text))
	_ = writer.Close()
	return buffer.Bytes()
}

const timedLines = `{"value":"a","timestamp":"2026-11-11T01:00:00Z"}
{"value":"b","timestamp":"2026-11-11T01:30:00.250Z"}
{"value":"c","timestamp":"2026-11-11T02:00:00Z"}
{"value":"d","timestamp":"2026-11-11T01:45:00Z"}
{"value":"e","timestamp":"2026-11-11T03:00:00Z"}
`

func windowBetween(from, to string) RecordWindow {
	start, _ := time.Parse(time.RFC3339, from)
	end, _ := time.Parse(time.RFC3339, to)
	return RecordWindow{From: start.UnixMilli(), To: end.UnixMilli()}
}

func TestScanObjectRecordsCountsEveryLineAndCaches(t *testing.T) {
	get := &fakeGet{bodies: map[string][]byte{"a.gz": gzipped("{\"value\":\"x\"}\n{\"value\":\"y\"}\n{\"value\":\"z\"}\n")}}
	scan := NewScanObjectRecords(get)
	for range 2 {
		got, err := scan.Execute(context.Background(), s3store.Object{Key: "a.gz", Size: 40}, AllRecords)
		if err != nil || got != (ObjectScan{Matched: 3, LastMatch: 3}) {
			t.Fatalf("scan = %+v, %v", got, err)
		}
	}
	if get.calls != 1 {
		t.Fatalf("object downloaded %d times", get.calls)
	}
}

func TestScanObjectRecordsReadsEveryCompression(t *testing.T) {
	const lines = "{\"value\":\"x\"}\n{\"value\":\"y\"}\n"
	get := &fakeGet{bodies: map[string][]byte{"o-0-00000000000000000000.zst": zstandard(lines), "o-0-00000000000000000002.snappy": xerialSnappy(lines),
		"o-0-00000000000000000004": []byte(lines)}}
	scan := NewScanObjectRecords(get)
	for key := range get.bodies {
		if got, err := scan.Execute(context.Background(), s3store.Object{Key: key}, AllRecords); err != nil || got.Matched != 2 {
			t.Errorf("%s: %+v %v", key, got, err)
		}
	}
}

func TestScanObjectRecordsInAWindow(t *testing.T) {
	get := &fakeGet{bodies: map[string][]byte{"t.gz": gzipped(timedLines)}}
	scan := NewScanObjectRecords(get)
	for window, want := range map[RecordWindow]ObjectScan{
		windowBetween("2026-11-11T01:00:00Z", "2026-11-11T02:00:00Z"): {Matched: 3, LastMatch: 4},
		windowBetween("2026-11-11T02:00:00Z", "2026-11-11T04:00:00Z"): {Matched: 2, LastMatch: 5},
		windowBetween("2026-11-11T01:00:00Z", "2026-11-11T01:00:01Z"): {Matched: 1, LastMatch: 1},
		windowBetween("2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z"): {},
	} {
		if got, err := scan.Execute(context.Background(), s3store.Object{Key: "t.gz", Size: 1}, window); err != nil || got != want {
			t.Errorf("%+v: %+v %v, want %+v", window, got, err, want)
		}
	}
	if get.calls != 4 {
		t.Fatalf("each window is scanned once: %d", get.calls)
	}
}

func TestScanObjectRecordsFailures(t *testing.T) {
	get := &fakeGet{bodies: map[string][]byte{"plain.gz": []byte("not gzip"), "bad.zst": []byte("not zstd"),
		"junk.gz": gzipped("not json\n"), "untimed.gz": gzipped("{\"value\":\"x\"}\n")}}
	scan := NewScanObjectRecords(get)
	window := windowBetween("2026-11-11T01:00:00Z", "2026-11-11T02:00:00Z")
	for _, key := range []string{"plain.gz", "bad.zst", "junk.gz", "untimed.gz"} {
		if _, err := scan.Execute(context.Background(), s3store.Object{Key: key}, window); !errors.Is(err, ErrStorageUnavailable) {
			t.Errorf("%s: %v", key, err)
		}
	}
	if _, err := scan.Execute(context.Background(), s3store.Object{Key: "missing.gz"}, AllRecords); !errors.Is(err, s3store.ErrUnavailable) {
		t.Errorf("missing: %v", err)
	}
}
