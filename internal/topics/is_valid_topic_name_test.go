package topics

import (
	"strings"
	"testing"
)

func TestIsValidTopicName(t *testing.T) {
	for _, name := range []string{"orders", "a._-Z09", strings.Repeat("x", 249)} {
		if !IsValidTopicName(name) {
			t.Errorf("%q should be valid", name)
		}
	}
	for _, name := range []string{"", ".", "..", "hello world", "café", "a/b", strings.Repeat("x", 250)} {
		if IsValidTopicName(name) {
			t.Errorf("%q should be invalid", name)
		}
	}
}
