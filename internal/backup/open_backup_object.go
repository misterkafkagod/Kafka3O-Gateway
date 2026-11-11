package backup

import (
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

// OpenBackupObject returns the object's decompressed jsonl lines for its compression (gzip, zstd, snappy or none).
func OpenBackupObject(body io.Reader, compression string) (io.ReadCloser, error) {
	switch compression {
	case "gzip":
		return gzip.NewReader(body)
	case "zstd":
		decoder, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(256<<20))
		if err != nil {
			return nil, err
		}
		return decoder.IOReadCloser(), nil
	case "snappy":
		return io.NopCloser(NewXerialSnappyReader(body)), nil
	default:
		return io.NopCloser(body), nil
	}
}
