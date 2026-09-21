package scan_test

import (
	"encoding/base64"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/scan"
)

func TestDecode_JSONThenUTF8ThenBase64(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		b    []byte
		want string
	}{
		{"json object", []byte(`{"a":1}`), scan.EncodingJSON},
		{"json array", []byte(`[1,2,3]`), scan.EncodingJSON},
		{"plain utf8 text", []byte("hello world"), scan.EncodingString},
		{"invalid utf8 binary", []byte{0xff, 0xfe, 0x00, 0x01}, scan.EncodingBase64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := scan.Decode(kafka.Record{Value: tc.b}, "")
			if r.ValueEncoding != tc.want {
				t.Errorf("ValueEncoding = %q, want %q", r.ValueEncoding, tc.want)
			}
		})
	}
}

func TestDecode_FormatOverrideForcesEncoding(t *testing.T) {
	t.Parallel()
	// Valid JSON, but format=base64 forces base64 regardless of what
	// auto-detection would have chosen (FUNC-SPEC §8.2 `format=` override).
	raw := []byte(`{"a":1}`)
	r := scan.Decode(kafka.Record{Value: raw}, scan.EncodingBase64)
	if r.ValueEncoding != scan.EncodingBase64 {
		t.Fatalf("ValueEncoding = %q, want base64", r.ValueEncoding)
	}
	if want := base64.StdEncoding.EncodeToString(raw); r.Value != want {
		t.Errorf("Value = %q, want %q", r.Value, want)
	}
}

func TestDecode_HeaderValuesStringOrBase64(t *testing.T) {
	t.Parallel()
	r := scan.Decode(kafka.Record{
		Headers: []kafka.Header{
			{Key: "h1", Value: []byte("text")},
			{Key: "h2", Value: []byte{0xff, 0xfe}},
			{Key: "h3", Value: []byte(`{"a":1}`)},
		},
	}, "")
	if r.Headers[0].ValueEncoding != scan.EncodingString || r.Headers[0].Value != "text" {
		t.Errorf("Headers[0] = %+v, want string/text", r.Headers[0])
	}
	if r.Headers[1].ValueEncoding != scan.EncodingBase64 {
		t.Errorf("Headers[1] = %+v, want base64", r.Headers[1])
	}
	// Headers never get the JSON check the record value gets.
	if r.Headers[2].ValueEncoding != scan.EncodingString {
		t.Errorf("Headers[2] with JSON-looking bytes = %+v, want string (no JSON detection for headers)", r.Headers[2])
	}
}

func TestDecode_SizeBytes(t *testing.T) {
	t.Parallel()
	r := scan.Decode(kafka.Record{Key: []byte("key"), Value: []byte("value123")}, "")
	if want := len("key") + len("value123"); r.SizeBytes != want {
		t.Errorf("SizeBytes = %d, want %d", r.SizeBytes, want)
	}
}
