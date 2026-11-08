package authorization

// IsKafkaMutation reports whether an action writes to Kafka; unknown actions are not mutations.
func IsKafkaMutation(action Action) bool {
	category, err := classifyAction(action)
	return err == nil && category == kafkaWrite
}
