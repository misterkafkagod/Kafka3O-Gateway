package backup

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/snappy"
)

var xerialMagic = []byte{0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0}

// maxXerialChunk bounds one compressed chunk; the sink writes chunks of at most 32 KiB of input.
const maxXerialChunk = 8 << 20

// XerialSnappyReader decompresses the xerial snappy stream the Aiven sink writes: an 8-byte magic, two 4-byte
// versions, then chunks of a 4-byte big-endian length and a raw snappy block.
type XerialSnappyReader struct {
	source  io.Reader
	started bool
	pending []byte
}

func NewXerialSnappyReader(source io.Reader) *XerialSnappyReader {
	return &XerialSnappyReader{source: source}
}

func (r *XerialSnappyReader) Read(out []byte) (int, error) {
	if !r.started {
		header := make([]byte, 16)
		if _, err := io.ReadFull(r.source, header); err != nil || !bytes.Equal(header[:8], xerialMagic) {
			return 0, errors.New("not a xerial snappy stream")
		}
		r.started = true
	}
	for len(r.pending) == 0 {
		var length [4]byte
		if _, err := io.ReadFull(r.source, length[:]); err == io.EOF {
			return 0, io.EOF
		} else if err != nil {
			return 0, errors.New("truncated xerial snappy chunk length")
		}
		size := binary.BigEndian.Uint32(length[:])
		if size == 0 || size > maxXerialChunk {
			return 0, errors.New("invalid xerial snappy chunk length")
		}
		chunk := make([]byte, size)
		if _, err := io.ReadFull(r.source, chunk); err != nil {
			return 0, errors.New("truncated xerial snappy chunk")
		}
		decoded, err := snappy.Decode(nil, chunk)
		if err != nil {
			return 0, err
		}
		r.pending = decoded
	}
	n := copy(out, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
