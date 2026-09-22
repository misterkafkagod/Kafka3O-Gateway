package scan

import (
	"encoding/base64"
	"fmt"
)

// EncodeValue converts a wire string and its declared encoding into raw
// bytes — the inverse of Decode (FUNC-SPEC §8.2), used when producing a
// record from a request body (M5-M7). An empty encoding defaults to
// "string"; "json" and "string" both pass value through unchanged (the wire
// text already is the record's raw bytes); "base64" decodes it.
func EncodeValue(value, encoding string) ([]byte, error) {
	switch encoding {
	case "", EncodingString, EncodingJSON:
		return []byte(value), nil
	case EncodingBase64:
		b, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 value: %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("invalid encoding %q: want json, string, or base64", encoding)
	}
}
