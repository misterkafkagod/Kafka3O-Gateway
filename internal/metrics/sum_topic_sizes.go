package metrics

import "slices"

// SumTopicSizes adds each topic's replica sizes over every log directory, largest first.
func SumTopicSizes(dirs []LogDir) []TopicSize {
	totals := map[string]int64{}
	for _, dir := range dirs {
		for topic, bytes := range dir.TopicBytes {
			totals[topic] += bytes
		}
	}
	sizes := make([]TopicSize, 0, len(totals))
	for topic, bytes := range totals {
		sizes = append(sizes, TopicSize{Topic: topic, Bytes: bytes})
	}
	slices.SortFunc(sizes, CompareTopicSizes)
	return sizes
}
