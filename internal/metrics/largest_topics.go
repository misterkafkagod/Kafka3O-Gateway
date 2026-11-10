package metrics

// largestTopicCount is how many topics the Metrics tab lists by size.
const largestTopicCount = 10

// LargestTopics keeps the largest topics by size on disk.
func LargestTopics(dirs []LogDir) []TopicSize {
	sizes := SumTopicSizes(dirs)
	return sizes[:min(len(sizes), largestTopicCount)]
}
