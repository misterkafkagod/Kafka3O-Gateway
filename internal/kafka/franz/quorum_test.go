package franz

import "testing"

// TestFranz_QuorumRequestBuiltFromKmsg proves C6's raw
// kmsg.DescribeQuorumRequest always targets the one topic-partition KRaft
// actually replicates as its quorum log (TECH-SPEC §1.1): no live broker is
// needed to check this, since it is pure request construction.
func TestFranz_QuorumRequestBuiltFromKmsg(t *testing.T) {
	t.Parallel()
	req := buildDescribeQuorumRequest()

	if len(req.Topics) != 1 || req.Topics[0].Topic != metadataQuorumTopic {
		t.Fatalf("req.Topics = %+v, want one entry for %q", req.Topics, metadataQuorumTopic)
	}
	partitions := req.Topics[0].Partitions
	if len(partitions) != 1 || partitions[0].Partition != 0 {
		t.Fatalf("req.Topics[0].Partitions = %+v, want one entry for partition 0", partitions)
	}
}
