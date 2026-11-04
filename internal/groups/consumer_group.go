package groups

type ConsumerGroupMember struct {
	ID         string  `json:"id"`
	ClientID   string  `json:"clientId"`
	ClientHost string  `json:"clientHost"`
	InstanceID *string `json:"instanceId,omitempty"`
}

type ConsumerGroupPartition struct {
	Topic           string  `json:"topic"`
	Partition       int32   `json:"partition"`
	CommittedOffset *string `json:"committedOffset"`
	StartOffset     *string `json:"startOffset"`
	EndOffset       *string `json:"endOffset"`
	Lag             *string `json:"lag"`
	Status          string  `json:"status"`
	Error           string  `json:"error,omitempty"`
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
