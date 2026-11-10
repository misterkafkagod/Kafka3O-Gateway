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

type offsetResetApplyPartition struct {
	Partition int32 `json:"partition" minimum:"0"`
	// ExpectedOffset is the committed offset the preview showed; omit or null when it showed no commit.
	ExpectedOffset *string `json:"expectedOffset,omitempty" pattern:"^[0-9]{1,19}$"`
	TargetOffset   string  `json:"targetOffset" pattern:"^[0-9]{1,19}$"`
}

type offsetResetApplyBody struct {
	Topic      string                      `json:"topic" minLength:"1"`
	Partitions []offsetResetApplyPartition `json:"partitions" minItems:"1" maxItems:"100000"`
}

type offsetResetCommitted struct {
	Partition       int32  `json:"partition"`
	CommittedOffset string `json:"committedOffset"`
}

type offsetResetAppliedBody struct {
	Group      string                 `json:"group"`
	Topic      string                 `json:"topic"`
	Partitions []offsetResetCommitted `json:"partitions"`
}
