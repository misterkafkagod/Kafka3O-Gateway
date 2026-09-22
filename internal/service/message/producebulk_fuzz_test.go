package message

import "testing"

// FuzzNDJSONParser proves parseNDJSON never panics on arbitrary bytes,
// whatever it decides about validity (TECH-SPEC §4.6) — M6's body is
// untrusted client input parsed line by line.
func FuzzNDJSONParser(f *testing.F) {
	for _, seed := range []string{
		"",
		"{\"value\":\"a\"}\n",
		"{\"value\":\"a\"}\n{\"value\":\"b\"}\n",
		"{\"value\":\"a\"}\n\n{\"value\":\"b\"}\n",
		"not json\n",
		"{\"value\":",
		"{}",
		"\x00\x01\x02\n{\"value\":\"a\"}\n",
		"日本語\n{\"value\":\"a\"}\n",
		"{\"value\":\"a\",\"partition\":-1}\n",
	} {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = parseNDJSON(data)
	})
}
