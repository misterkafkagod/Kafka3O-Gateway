// Package scan implements the bounded-scan state machine shared by M1, M3,
// and M4 (FUNC-SPEC §9.2): resolve a per-partition window, assign, poll,
// decode, match, and stop at whichever bound (or the end snapshot) comes
// first. It holds no kafka.Admin or kafka.Producer role, and never imports
// internal/config or internal/service — the caller (Task 3.3's
// internal/service/message) resolves `from=`/`to=` into concrete offsets
// before building a Spec.
package scan

import (
	"encoding/base64"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Encoding values (FUNC-SPEC §8.2).
const (
	EncodingJSON   = "json"
	EncodingString = "string"
	EncodingBase64 = "base64"
)

// Header is one decoded record header (FUNC-SPEC §8.3 Record row).
type Header struct {
	Key           string
	Value         string
	ValueEncoding string
}

// Record is one decoded, wire-ready message (FUNC-SPEC §8.3 Record row). The
// API layer (Task 3.4) renders Timestamp as both `timestamp` (ISO-8601) and
// `timestampMs`.
type Record struct {
	Topic         string
	Partition     int32
	Offset        int64
	Timestamp     time.Time
	Key           string
	KeyEncoding   string
	Value         string
	ValueEncoding string
	Headers       []Header
	SizeBytes     int
}

// Decode converts a raw kafka.Record into the wire-ready Record shape,
// auto-detecting key and value as JSON, then UTF-8 text, then base64
// (FUNC-SPEC §8.2), unless format is a non-empty override that forces one
// encoding. SizeBytes is the raw key+value byte length.
func Decode(r kafka.Record, format string) Record {
	key, keyEncoding := decodeBytes(r.Key, format)
	value, valueEncoding := decodeBytes(r.Value, format)

	headers := make([]Header, len(r.Headers))
	for i, h := range r.Headers {
		v, enc := decodeHeaderValue(h.Value)
		headers[i] = Header{Key: h.Key, Value: v, ValueEncoding: enc}
	}

	return Record{
		Topic:         r.Topic,
		Partition:     r.Partition,
		Offset:        r.Offset,
		Timestamp:     r.Timestamp,
		Key:           key,
		KeyEncoding:   keyEncoding,
		Value:         value,
		ValueEncoding: valueEncoding,
		Headers:       headers,
		SizeBytes:     len(r.Key) + len(r.Value),
	}
}

// decodeBytes renders b per format when format names a specific encoding b
// actually satisfies, otherwise per the JSON -> UTF-8 -> base64 auto-detect
// order (FUNC-SPEC §8.2).
func decodeBytes(b []byte, format string) (string, string) {
	switch format {
	case EncodingBase64:
		return base64.StdEncoding.EncodeToString(b), EncodingBase64
	case EncodingJSON:
		if json.Valid(b) {
			return string(b), EncodingJSON
		}
	case EncodingString:
		if utf8.Valid(b) {
			return string(b), EncodingString
		}
	}
	return autoDecode(b)
}

// autoDecode is the FUNC-SPEC §8.2 auto-detect order: JSON, then UTF-8 text,
// then base64.
func autoDecode(b []byte) (string, string) {
	if json.Valid(b) {
		return string(b), EncodingJSON
	}
	if utf8.Valid(b) {
		return string(b), EncodingString
	}
	return base64.StdEncoding.EncodeToString(b), EncodingBase64
}

// decodeHeaderValue applies the header rule (FUNC-SPEC §8.2): string if
// valid UTF-8, else base64 — no JSON detection for headers.
func decodeHeaderValue(b []byte) (string, string) {
	if utf8.Valid(b) {
		return string(b), EncodingString
	}
	return base64.StdEncoding.EncodeToString(b), EncodingBase64
}
