package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"testing"

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

func TestCountObjectRecordsCountsLinesAndCaches(t *testing.T) {
	get := &fakeGet{bodies: map[string][]byte{"a.gz": gzipped("{\"value\":\"x\"}\n{\"value\":\"y\"}\n{\"value\":\"z\"}\n")}}
	count := NewCountObjectRecords(get)
	for range 2 {
		n, err := count.Execute(context.Background(), s3store.Object{Key: "a.gz", Size: 40})
		if err != nil || n != 3 {
			t.Fatalf("count = %d, %v", n, err)
		}
	}
	if get.calls != 1 {
		t.Fatalf("object downloaded %d times", get.calls)
	}
}

func TestCountObjectRecordsReadsEveryCompression(t *testing.T) {
	const lines = "{\"value\":\"x\"}\n{\"value\":\"y\"}\n"
	get := &fakeGet{bodies: map[string][]byte{"o-0-00000000000000000000.zst": zstandard(lines), "o-0-00000000000000000002.snappy": xerialSnappy(lines),
		"o-0-00000000000000000004": []byte(lines)}}
	count := NewCountObjectRecords(get)
	for key := range get.bodies {
		if n, err := count.Execute(context.Background(), s3store.Object{Key: key}); err != nil || n != 2 {
			t.Errorf("%s: %d %v", key, n, err)
		}
	}
}

func TestCountObjectRecordsFailures(t *testing.T) {
	get := &fakeGet{bodies: map[string][]byte{"plain.gz": []byte("not gzip"), "bad.zst": []byte("not zstd")}}
	count := NewCountObjectRecords(get)
	if _, err := count.Execute(context.Background(), s3store.Object{Key: "plain.gz"}); !errors.Is(err, ErrStorageUnavailable) {
		t.Errorf("not gzip: %v", err)
	}
	if _, err := count.Execute(context.Background(), s3store.Object{Key: "bad.zst"}); !errors.Is(err, ErrStorageUnavailable) {
		t.Errorf("not zstd: %v", err)
	}
	if _, err := count.Execute(context.Background(), s3store.Object{Key: "missing.gz"}); !errors.Is(err, s3store.ErrUnavailable) {
		t.Errorf("missing: %v", err)
	}
}
