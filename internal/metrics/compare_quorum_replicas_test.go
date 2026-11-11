package metrics

import (
	"slices"
	"testing"
)

func TestCompareQuorumReplicas(t *testing.T) {
	replicas := []QuorumReplica{{ID: 1, Role: "observer"}, {ID: 3, Role: "voter"}, {ID: 2, Role: "voter"}}
	slices.SortFunc(replicas, CompareQuorumReplicas)
	if replicas[0].ID != 2 || replicas[1].ID != 3 || replicas[2].Role != "observer" {
		t.Fatalf("order = %+v", replicas)
	}
}
