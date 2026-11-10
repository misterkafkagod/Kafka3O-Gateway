package metrics

import (
	"slices"
	"testing"
)

func count(n int) *int { return &n }

func TestSummarizePartitionHealth_AddsTopicsAndListsUnreadableOnes(t *testing.T) {
	got := SummarizePartitionHealth([]TopicHealth{
		{Topic: "orders", Partitions: 3, UnderReplicated: 2, Offline: 1, BelowMinISR: count(2)},
		{Topic: "__consumer_offsets", Partitions: 50, BelowMinISR: count(0)},
		{Topic: "zeta", Error: "TOPIC_AUTHORIZATION_FAILED"},
		{Topic: "alpha", Error: "UNKNOWN_TOPIC_OR_PARTITION"},
	})
	if got.Topics != 2 || got.Partitions != 53 || got.UnderReplicated != 2 || got.Offline != 1 || got.BelowMinISR == nil || *got.BelowMinISR != 2 || got.MinISRError != "" {
		t.Fatalf("summary = %+v", got)
	}
	if !slices.Equal(got.UnreadableTopics, []string{"alpha", "zeta"}) {
		t.Fatalf("unreadable = %v", got.UnreadableTopics)
	}
}

func TestSummarizePartitionHealth_LeavesBelowMinISRUnknownWithTheReason(t *testing.T) {
	got := SummarizePartitionHealth([]TopicHealth{
		{Topic: "orders", Partitions: 1, BelowMinISR: count(1)},
		{Topic: "payments", Partitions: 1, MinISRError: "read topic configuration: denied"},
	})
	if got.BelowMinISR != nil || got.MinISRError != "read topic configuration: denied" || got.Partitions != 2 {
		t.Fatalf("summary = %+v", got)
	}
}
