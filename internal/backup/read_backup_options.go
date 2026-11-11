package backup

import "strconv"

// ReadBackupOptions reads the user-chosen settings back from a backup connector's configuration; missing keys mean
// the M13 defaults (from the oldest message, gzip, no record limit).
func ReadBackupOptions(config map[string]string) BackupOptions {
	options := BackupOptions{StartFrom: "earliest", Compression: "gzip"}
	if value := config["consumer.override.auto.offset.reset"]; value != "" {
		options.StartFrom = value
	}
	if value := config["file.compression.type"]; value != "" {
		options.Compression = value
	}
	if records, err := strconv.Atoi(config["file.max.records"]); err == nil && records > 0 {
		options.RecordsPerFile = records
	}
	return options
}
