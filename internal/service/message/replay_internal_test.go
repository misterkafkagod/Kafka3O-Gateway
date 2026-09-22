package message

import (
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/scan"
)

// TestReplay_PartitionByKeyUnlessPreserved proves replayProduceRequest sets
// an explicit target Partition only when preservePartition is true — the
// producer's own key-based partitioner decides otherwise (FUNC-SPEC §9.3
// step 3). Package-internal since replayProduceRequest is unexported.
func TestReplay_PartitionByKeyUnlessPreserved(t *testing.T) {
	t.Parallel()
	item := scan.Record{Partition: 3, Value: "v", ValueEncoding: "string"}

	notPreserved, err := replayProduceRequest(item, false)
	if err != nil {
		t.Fatalf("replayProduceRequest(preserve=false) error: %v", err)
	}
	if notPreserved.Partition != nil {
		t.Errorf("Partition = %v, want nil (key-based partitioning) when not preserving", notPreserved.Partition)
	}

	preserved, err := replayProduceRequest(item, true)
	if err != nil {
		t.Fatalf("replayProduceRequest(preserve=true) error: %v", err)
	}
	if preserved.Partition == nil || *preserved.Partition != 3 {
		t.Errorf("Partition = %v, want 3 (preserved from source) when preserving", preserved.Partition)
	}
}
