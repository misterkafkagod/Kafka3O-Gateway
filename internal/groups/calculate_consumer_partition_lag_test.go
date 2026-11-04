package groups

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCalculateConsumerPartitionLag(t *testing.T) {
	start, end, commit := int64(4), int64(20), int64(12)
	tests := []struct {
		name     string
		snapshot ConsumerPartitionSnapshot
		status   string
		lag      *string
		error    bool
	}{
		{name: "valid", snapshot: ConsumerPartitionSnapshot{StartOffset: &start, EndOffset: &end, CommittedOffset: &commit}, status: "ok", lag: new("8")},
		{name: "no commit", snapshot: ConsumerPartitionSnapshot{StartOffset: &start, EndOffset: &end}, status: "no_commit"},
		{name: "sentinel commit", snapshot: ConsumerPartitionSnapshot{StartOffset: &start, EndOffset: &end, CommittedOffset: new(int64(-1))}, status: "no_commit"},
		{name: "before retention", snapshot: ConsumerPartitionSnapshot{StartOffset: &start, EndOffset: &end, CommittedOffset: new(int64(3))}, status: "out_of_range"},
		{name: "ahead of end", snapshot: ConsumerPartitionSnapshot{StartOffset: &start, EndOffset: &end, CommittedOffset: new(int64(21))}, status: "out_of_range"},
		{name: "bad commit", snapshot: ConsumerPartitionSnapshot{StartOffset: &start, EndOffset: &end, CommittedOffset: new(int64(-2))}, status: "unavailable", error: true},
		{name: "missing offsets", snapshot: ConsumerPartitionSnapshot{StartOffset: &start}, status: "unavailable", error: true},
		{name: "bad bounds", snapshot: ConsumerPartitionSnapshot{StartOffset: &end, EndOffset: &start}, status: "unavailable", error: true},
		{name: "broker error", snapshot: ConsumerPartitionSnapshot{Error: "not authorized"}, status: "unavailable", error: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateConsumerPartitionLag(tt.snapshot)
			if got.Status != tt.status || (got.Error != "") != tt.error {
				t.Errorf("status = %q, error = %q", got.Status, got.Error)
			}
			if (got.Lag == nil) != (tt.lag == nil) || (got.Lag != nil && *got.Lag != *tt.lag) {
				t.Errorf("lag = %v, want %v", got.Lag, tt.lag)
			}
			if tt.status == "no_commit" && got.CommittedOffset != nil {
				t.Errorf("committed offset = %v, want nil", got.CommittedOffset)
			}
		})
	}
}

func TestCalculateConsumerPartitionLag_SerializesInt64AsStrings(t *testing.T) {
	start, end, commit := int64(0), int64(9007199254740995), int64(9007199254740993)
	body, err := json.Marshal(calculateConsumerPartitionLag(ConsumerPartitionSnapshot{
		Topic: "orders", StartOffset: &start, EndOffset: &end, CommittedOffset: &commit,
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"committedOffset":"9007199254740993"`, `"endOffset":"9007199254740995"`, `"lag":"2"`} {
		if !strings.Contains(string(body), field) {
			t.Errorf("JSON %s does not contain %s", body, field)
		}
	}
}
