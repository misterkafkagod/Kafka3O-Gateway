package topics

type TopicMessagePosition struct {
	Partition int32
	Offset    *int64
	Timestamp *int64
}
