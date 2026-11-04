package groups

func calculateConsumerPartitionLag(snapshot ConsumerPartitionSnapshot) ConsumerGroupPartition {
	result := ConsumerGroupPartition{
		Topic:           snapshot.Topic,
		Partition:       snapshot.Partition,
		CommittedOffset: consumerOffsetString(snapshot.CommittedOffset),
		StartOffset:     consumerOffsetString(snapshot.StartOffset),
		EndOffset:       consumerOffsetString(snapshot.EndOffset),
	}
	if snapshot.Error != "" {
		result.Status = "unavailable"
		result.Error = snapshot.Error
		return result
	}
	if snapshot.StartOffset == nil || snapshot.EndOffset == nil {
		result.Status = "unavailable"
		result.Error = "log start or end offset is unavailable"
		return result
	}
	start, end := *snapshot.StartOffset, *snapshot.EndOffset
	if start < 0 || end < start {
		result.Status = "unavailable"
		result.Error = "invalid log offset range"
		return result
	}
	if snapshot.CommittedOffset == nil || *snapshot.CommittedOffset == -1 {
		result.Status = "no_commit"
		result.CommittedOffset = nil
		return result
	}
	committed := *snapshot.CommittedOffset
	if committed < 0 {
		result.Status = "unavailable"
		result.Error = "invalid committed offset"
		return result
	}
	if committed < start || committed > end {
		result.Status = "out_of_range"
		return result
	}
	lag := end - committed
	result.Lag = consumerOffsetString(&lag)
	result.Status = "ok"
	return result
}
