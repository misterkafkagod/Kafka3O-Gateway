package scan_test

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/scan"
)

// FuzzDecode proves Decode never panics on arbitrary key/value bytes or
// format override, and that its result is always one of the three declared
// encodings (TECH-SPEC §4.6).
func FuzzDecode(f *testing.F) {
	for _, seed := range []struct {
		b      []byte
		format string
	}{
		{[]byte(`{"a":1}`), ""},
		{[]byte("plain text"), ""},
		{[]byte{0xff, 0xfe, 0x00}, ""},
		{[]byte(`{"a":1}`), scan.EncodingBase64},
		{nil, ""},
		{[]byte("x"), "bogus-format"},
	} {
		f.Add(seed.b, seed.format)
	}

	f.Fuzz(func(t *testing.T, value []byte, format string) {
		r := scan.Decode(kafka.Record{Value: value}, format)
		switch r.ValueEncoding {
		case scan.EncodingJSON, scan.EncodingString, scan.EncodingBase64:
		default:
			t.Fatalf("Decode(%q, %q).ValueEncoding = %q, want one of json/string/base64", value, format, r.ValueEncoding)
		}
	})
}

// FuzzHeaderEncoding proves header-value decoding never panics on arbitrary
// bytes and always resolves to string or base64 — never json (FUNC-SPEC
// §8.2: no JSON detection for headers; TECH-SPEC §4.6).
func FuzzHeaderEncoding(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("text"),
		{0xff, 0xfe},
		nil,
		[]byte(`{"looks":"like json"}`),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, headerValue []byte) {
		r := scan.Decode(kafka.Record{Headers: []kafka.Header{{Key: "h", Value: headerValue}}}, "")
		enc := r.Headers[0].ValueEncoding
		if enc != scan.EncodingString && enc != scan.EncodingBase64 {
			t.Fatalf("header ValueEncoding = %q, want string or base64", enc)
		}
	})
}
