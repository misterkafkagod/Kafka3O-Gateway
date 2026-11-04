package topics

import "time"

type TopicMessage struct {
	Partition   int32                `json:"partition"`
	Offset      int64                `json:"offset"`
	OffsetExact string               `json:"offsetExact" doc:"Exact decimal Kafka offset"`
	Timestamp   time.Time            `json:"timestamp"`
	Key         []byte               `json:"key"`
	Value       []byte               `json:"value"`
	Headers     []TopicMessageHeader `json:"headers"`
}

type TopicMessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
