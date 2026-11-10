package backup

// BackupLocation is where a topic's backup objects are stored, as shown to users.
func BackupLocation(settings Settings, topic string) string {
	return "s3://" + settings.Bucket + "/" + settings.Prefix + topic + "/"
}
