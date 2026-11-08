package identity

import (
	"strings"
	"testing"
)

func TestValidChangeReason(t *testing.T) {
	for _, tt := range []struct {
		reason string
		valid  bool
	}{
		{"Customer-facing cluster", true}, {strings.Repeat("r", 512), true}, {"Größe ändern", true},
		{"", false}, {" padded", false}, {"padded ", false}, {"line\nbreak", false},
		{strings.Repeat("r", 513), false}, {string([]byte{0xff}), false},
	} {
		if got := validChangeReason(tt.reason); got != tt.valid {
			t.Errorf("validChangeReason(%q) = %t, want %t", tt.reason, got, tt.valid)
		}
	}
}
