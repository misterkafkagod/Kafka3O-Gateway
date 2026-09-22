package scan_test

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/scan"
)

// FuzzJSONPathCompile proves NewJSONPathMatcher's path compile step never
// panics on arbitrary JSONPath syntax, whatever it decides about validity
// (TECH-SPEC §4.6).
func FuzzJSONPathCompile(f *testing.F) {
	for _, seed := range []string{
		"$.status", "$.items[*].status", "$..status", "$['a']['b']",
		"$.a[0:10:2]", "$", "", "not a path", "$.[", "$['unterminated",
		"$.a?(@.b==1)", "日本語",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, path string) {
		_, _ = scan.NewJSONPathMatcher(path, scan.OpExists, nil)
	})
}
