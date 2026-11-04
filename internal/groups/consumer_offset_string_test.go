package groups

import "testing"

func TestConsumerOffsetString(t *testing.T) {
	if consumerOffsetString(nil) != nil {
		t.Fatal("nil offset must remain nil")
	}
	got := consumerOffsetString(new(int64(9007199254740993)))
	if got == nil || *got != "9007199254740993" {
		t.Errorf("offset = %v", got)
	}
}
