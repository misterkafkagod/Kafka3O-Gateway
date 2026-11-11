package metrics

import (
	"math/big"
	"slices"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

// SummarizeConsumerGroups counts groups by state and sums each group's known partition lag exactly; partitions
// without a known lag are counted, never added as zero.
func SummarizeConsumerGroups(consumerGroups []groups.ConsumerGroup) Groups {
	summary := Groups{States: []StateCount{}, Groups: make([]GroupLag, 0, len(consumerGroups))}
	states := map[string]int{}
	for _, group := range consumerGroups {
		states[group.State]++
		entry := GroupLag{Group: group.ID, State: group.State, Members: len(group.Members), Error: group.Error}
		total, largest, known := new(big.Int), new(big.Int), false
		for _, partition := range group.Partitions {
			lag, ok := new(big.Int), partition.Lag != nil
			if ok {
				_, ok = lag.SetString(*partition.Lag, 10)
			}
			if !ok {
				entry.UnknownPartitions++
				continue
			}
			known = true
			total.Add(total, lag)
			if lag.Cmp(largest) > 0 {
				largest.Set(lag)
			}
		}
		if known {
			sum, top := total.String(), largest.String()
			entry.Lag, entry.MaxPartitionLag = &sum, &top
		}
		summary.Groups = append(summary.Groups, entry)
	}
	for state, count := range states {
		summary.States = append(summary.States, StateCount{State: state, Count: count})
	}
	slices.SortFunc(summary.States, CompareStateCounts)
	slices.SortFunc(summary.Groups, CompareGroupLags)
	return summary
}
