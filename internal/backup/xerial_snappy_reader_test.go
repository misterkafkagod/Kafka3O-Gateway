package backup

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/klauspost/compress/snappy"
)

// xerialSnappy builds the layout seen in the backup-settings-and-restore-filters discovery: magic, version 1,
// compatible version 1, then length-prefixed snappy blocks.
func xerialSnappy(chunks ...string) []byte {
	var buffer bytes.Buffer
	buffer.Write(xerialMagic)
	_ = binary.Write(&buffer, binary.BigEndian, int32(1))
	_ = binary.Write(&buffer, binary.BigEndian, int32(1))
	for _, chunk := range chunks {
		block := snappy.Encode(nil, []byte(chunk))
		_ = binary.Write(&buffer, binary.BigEndian, uint32(len(block)))
		buffer.Write(block)
	}
	return buffer.Bytes()
}

func TestXerialSnappyReaderDecodesEveryChunk(t *testing.T) {
	got, err := io.ReadAll(NewXerialSnappyReader(bytes.NewReader(xerialSnappy("{\"value\":\"a\"}\n", "{\"value\":\"b\"}\n"))))
	if err != nil || string(got) != "{\"value\":\"a\"}\n{\"value\":\"b\"}\n" {
		t.Fatalf("%q %v", got, err)
	}
	if got, err := io.ReadAll(NewXerialSnappyReader(bytes.NewReader(xerialSnappy()))); err != nil || len(got) != 0 {
		t.Fatalf("empty stream: %q %v", got, err)
	}
}

func TestXerialSnappyReaderRefusesCorruptStreams(t *testing.T) {
	valid := xerialSnappy("{\"value\":\"a\"}\n")
	// Snappy blocks carry no checksum; a wrong decoded-length header is what the decoder can detect.
	corruptBlock := append([]byte{}, valid...)
	corruptBlock[20] = 0x7f
	for name, stream := range map[string][]byte{
		"not snappy":    []byte("plain text, not a snappy stream"),
		"short header":  valid[:10],
		"truncated":     valid[:len(valid)-2],
		"huge length":   append(append([]byte{}, valid[:16]...), 0x7f, 0xff, 0xff, 0xff),
		"corrupt block": corruptBlock,
	} {
		if _, err := io.ReadAll(NewXerialSnappyReader(bytes.NewReader(stream))); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
