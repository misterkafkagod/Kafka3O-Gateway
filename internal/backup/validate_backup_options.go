package backup

import "fmt"

// MaxRecordsPerFile bounds file.max.records; 0 means no limit (files at Kafka Connect's offset commit).
const MaxRecordsPerFile = 1000000

// ValidateBackupOptions fills the defaults (from the oldest message, gzip, no record limit) and refuses anything
// outside the offered choices, naming the field; only validated options reach a connector configuration.
func ValidateBackupOptions(options *BackupOptions) (BackupOptions, error) {
	valid := BackupOptions{StartFrom: "earliest", Compression: "gzip"}
	if options == nil {
		return valid, nil
	}
	switch options.StartFrom {
	case "":
	case "earliest", "latest":
		valid.StartFrom = options.StartFrom
	default:
		return BackupOptions{}, fmt.Errorf("%w: startFrom must be earliest or latest", ErrInvalidBackupSettings)
	}
	switch options.Compression {
	case "":
	case "gzip", "zstd", "snappy", "none":
		valid.Compression = options.Compression
	default:
		return BackupOptions{}, fmt.Errorf("%w: compression must be gzip, zstd, snappy or none", ErrInvalidBackupSettings)
	}
	if options.RecordsPerFile < 0 || options.RecordsPerFile > MaxRecordsPerFile {
		return BackupOptions{}, fmt.Errorf("%w: recordsPerFile must be between 0 and %d", ErrInvalidBackupSettings, MaxRecordsPerFile)
	}
	valid.RecordsPerFile = options.RecordsPerFile
	return valid, nil
}
