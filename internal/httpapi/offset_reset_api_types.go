package httpapi

// Offsets are strings so 64-bit values survive JavaScript clients.
type offsetResetPartition struct {
	Partition     int32   `json:"partition"`
	CurrentOffset *string `json:"currentOffset" doc:"Committed offset, or null when the group has no commit"`
	TargetOffset  string  `json:"targetOffset"`
}

type offsetResetPreviewBody struct {
	Group      string                 `json:"group"`
	Topic      string                 `json:"topic"`
	Partitions []offsetResetPartition `json:"partitions"`
}
