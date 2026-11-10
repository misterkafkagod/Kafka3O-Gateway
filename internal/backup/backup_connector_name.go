package backup

// BackupConnectorName names the one backup connector a topic may have.
func BackupConnectorName(topic string) string {
	return "kafka3o-backup-" + topic
}
