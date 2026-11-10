package metrics

import "testing"

func TestEscapeLabelValue_EscapesBackslashQuoteAndNewline(t *testing.T) {
	if got := EscapeLabelValue("a\\b\"c\nd"); got != `a\\b\"c\nd` {
		t.Fatalf("got %q", got)
	}
}
