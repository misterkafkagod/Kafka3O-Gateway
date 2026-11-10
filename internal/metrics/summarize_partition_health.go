package metrics

import "slices"

// SummarizePartitionHealth adds up topic health; below-min-ISR is a total only when every readable topic reported it.
func SummarizePartitionHealth(topics []TopicHealth) Partitions {
	summary := Partitions{UnreadableTopics: []string{}}
	belowMinISR := 0
	for _, topic := range topics {
		if topic.Error != "" {
			summary.UnreadableTopics = append(summary.UnreadableTopics, topic.Topic)
			continue
		}
		summary.Topics++
		summary.Partitions += topic.Partitions
		summary.UnderReplicated += topic.UnderReplicated
		summary.Offline += topic.Offline
		if topic.BelowMinISR == nil {
			if summary.MinISRError == "" {
				summary.MinISRError = topic.MinISRError
			}
			continue
		}
		belowMinISR += *topic.BelowMinISR
	}
	if summary.MinISRError == "" {
		summary.BelowMinISR = &belowMinISR
	}
	slices.Sort(summary.UnreadableTopics)
	return summary
}
