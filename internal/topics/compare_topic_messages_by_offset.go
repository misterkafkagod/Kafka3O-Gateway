package topics

func CompareTopicMessagesByOffset(a, b TopicMessage) int {
	switch {
	case a.Offset < b.Offset:
		return -1
	case a.Offset > b.Offset:
		return 1
	default:
		return 0
	}
}
