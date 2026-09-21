package message_test

import (
	"testing"

	apimessage "github.com/misterkafkagod/kafka3o/internal/api/message"
)

// FuzzParseFromTo proves ParseFrom never panics on arbitrary `from=`/`to=`
// wire syntax, whatever it decides about validity (TECH-SPEC §4.6).
func FuzzParseFromTo(f *testing.F) {
	for _, seed := range []string{
		"", "beginning", "latest", "offset:0", "offset:-1", "offset:9999999999999999999",
		"timestamp:0", "timestamp:1700000000000", "timestamp:2023-11-14T22:13:20Z",
		"timestamp:not-a-time", "offset:", "timestamp:", "OFFSET:1", "beginning ",
		"\x00\x01\x02", "日本語",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		_, _ = apimessage.ParseFrom(s)
	})
}
