package metrics

import "cmp"

// CompareQuorumReplicas orders voters before observers, then by replica id.
func CompareQuorumReplicas(a, b QuorumReplica) int {
	if a.Role != b.Role {
		if a.Role == "voter" {
			return -1
		}
		return 1
	}
	return cmp.Compare(a.ID, b.ID)
}
