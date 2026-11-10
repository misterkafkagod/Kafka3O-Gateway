package backup

// RestoreConnectorName names the one restore connector a topic may have at a time.
func RestoreConnectorName(topic string) string {
	return "kafka3o-restore-" + topic
}
