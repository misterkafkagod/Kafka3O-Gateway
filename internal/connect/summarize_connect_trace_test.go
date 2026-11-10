package connect

import (
	"strings"
	"testing"
)

func TestSummarizeConnectTrace(t *testing.T) {
	trace := "org.apache.kafka.connect.errors.ConnectException: Couldn't find or create file '/x'\n\tat a.b(C.java:74)\n\tat d.e(F.java:1)"
	if got := SummarizeConnectTrace(trace); got != "org.apache.kafka.connect.errors.ConnectException: Couldn't find or create file '/x'" {
		t.Fatalf("summary = %q", got)
	}
	if got := SummarizeConnectTrace(""); got != "" {
		t.Fatalf("empty trace = %q", got)
	}
	long := SummarizeConnectTrace(strings.Repeat("é", 700))
	if len([]rune(long)) != 501 || !strings.HasSuffix(long, "…") {
		t.Fatalf("long summary has %d runes", len([]rune(long)))
	}
}
