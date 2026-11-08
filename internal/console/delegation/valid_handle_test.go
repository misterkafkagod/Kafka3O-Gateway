package delegation

import (
	"strings"
	"testing"
)

func TestValidHandle(t *testing.T) {
	if !validHandle(strings.Repeat("A", 42) + "E") {
		t.Fatal("a 32-byte handle was rejected")
	}
	for _, handle := range []string{"", strings.Repeat("A", 42), strings.Repeat("A", 44), strings.Repeat("A", 42) + "B", strings.Repeat("+", 43)} {
		if validHandle(handle) {
			t.Fatalf("validHandle(%q) = true", handle)
		}
	}
}
