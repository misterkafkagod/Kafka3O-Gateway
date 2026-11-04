package topics

type TopicDetail struct {
	Name       string           `json:"name"`
	Partitions []TopicPartition `json:"partitions"`
	Config     []TopicConfig    `json:"config"`
}

type TopicPartition struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	StartOffset int64   `json:"startOffset"`
	EndOffset   int64   `json:"endOffset"`
}

type TopicConfig struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Sensitive bool   `json:"sensitive"`
}
