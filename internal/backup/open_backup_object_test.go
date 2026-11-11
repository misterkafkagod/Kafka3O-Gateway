package backup

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func zstandard(text string) []byte {
	encoder, _ := zstd.NewWriter(nil)
	return encoder.EncodeAll([]byte(text), nil)
}

func TestOpenBackupObjectForEachCompression(t *testing.T) {
	const lines = "{\"value\":\"a\"}\n{\"value\":\"b\"}\n"
	for compression, body := range map[string][]byte{
		"gzip": gzipped(lines), "zstd": zstandard(lines), "snappy": xerialSnappy(lines), "none": []byte(lines),
	} {
		reader, err := OpenBackupObject(bytes.NewReader(body), compression)
		if err != nil {
			t.Fatalf("%s: %v", compression, err)
		}
		got, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || string(got) != lines {
			t.Errorf("%s: %q %v", compression, got, err)
		}
	}
}

func TestOpenBackupObjectRefusesTheWrongCompression(t *testing.T) {
	for compression, body := range map[string][]byte{"gzip": []byte("plain"), "zstd": gzipped("x"), "snappy": zstandard("x")} {
		reader, err := OpenBackupObject(bytes.NewReader(body), compression)
		if err == nil {
			_, err = io.ReadAll(reader)
		}
		if err == nil {
			t.Errorf("%s accepted the wrong format", compression)
		}
	}
}
