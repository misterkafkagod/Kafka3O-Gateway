package groups

type ConsumerGroupMember struct {
	ID         string  `json:"id"`
	ClientID   string  `json:"clientId"`
	ClientHost string  `json:"clientHost"`
	InstanceID *string `json:"instanceId,omitempty"`
}

type ConsumerGroupPartition struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset *int64 `json:"committedOffset,string"`
	StartOffset     *int64 `json:"startOffset,string"`
	EndOffset       *int64 `json:"endOffset,string"`
	Lag             *int64 `json:"lag,string"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
}

type ConsumerGroup struct {
	ID         string                   `json:"id"`
	Protocol   string                   `json:"protocol"`
	State      string                   `json:"state"`
	Members    []ConsumerGroupMember    `json:"members"`
	Topics     []string                 `json:"topics"`
	Partitions []ConsumerGroupPartition `json:"partitions"`
	Error      string                   `json:"error,omitempty"`
}
