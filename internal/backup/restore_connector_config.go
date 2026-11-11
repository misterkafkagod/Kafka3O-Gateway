package backup

import "strconv"

var compressionExtensions = map[string]string{"gzip": ".gz", "zstd": ".zst", "snappy": ".snappy", "none": ""}

// RestoreConnectorConfig is the s3-backup-connectors recipe's source configuration with the Kafka3O restore transform.
// Each partition's objects go to one task in order, so restores run in parallel across partitions without reordering.
// The source reads only objects of the given compression (gzip, zstd, snappy or none), named with its extension.
// A window other than AllRecords is passed to the transform, which drops records outside it.
func RestoreConnectorConfig(settings Settings, topic, compression, restoreID, boundary string, partitions int, resumeBackup bool, window RecordWindow) map[string]string {
	tasks := partitions
	if tasks < 1 {
		tasks = 1
	}
	if tasks > 8 {
		tasks = 8
	}
	config := map[string]string{
		"connector.class":                     "io.aiven.kafka.connect.s3.source.S3SourceConnector",
		"tasks.max":                           strconv.Itoa(tasks),
		"distribution.type":                   "partition",
		"topic":                               topic,
		"aws.access.key.id":                   "${env:BACKUP_S3_ACCESS_KEY}",
		"aws.secret.access.key":               "${env:BACKUP_S3_SECRET_KEY}",
		"aws.s3.endpoint":                     settings.S3Endpoint,
		"aws.s3.region":                       settings.Region,
		"aws.s3.bucket.name":                  settings.Bucket,
		"aws.s3.prefix":                       settings.Prefix + topic + "/",
		"file.name.template":                  "{{topic}}-{{partition}}-{{start_offset}}" + compressionExtensions[compression],
		"file.compression.type":               compression,
		"input.format":                        "jsonl",
		"key.converter":                       byteArrayConverter,
		"value.converter":                     byteArrayConverter,
		"header.converter":                    byteArrayConverter,
		"transforms":                          "unwrap",
		"transforms.unwrap.type":              "dev.kafka3o.connect.UnwrapBackupRecord",
		"transforms.unwrap.restore.id":        restoreID,
		"transforms.unwrap.snapshot.boundary": boundary,
		"transforms.unwrap.resume.backup":     strconv.FormatBool(resumeBackup),
	}
	if window.From != AllRecords.From {
		config["transforms.unwrap.time.from"] = strconv.FormatInt(window.From, 10)
	}
	if window.To != AllRecords.To {
		config["transforms.unwrap.time.to"] = strconv.FormatInt(window.To, 10)
	}
	return config
}
