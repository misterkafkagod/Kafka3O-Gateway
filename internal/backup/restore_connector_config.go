package backup

import "strconv"

// RestoreConnectorConfig is the s3-backup-connectors recipe's source configuration with the Kafka3O restore transform.
// Each partition's objects go to one task in order, so restores run in parallel across partitions without reordering.
func RestoreConnectorConfig(settings Settings, topic, restoreID, boundary string, partitions int, resumeBackup bool) map[string]string {
	tasks := partitions
	if tasks < 1 {
		tasks = 1
	}
	if tasks > 8 {
		tasks = 8
	}
	return map[string]string{
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
		"file.name.template":                  "{{topic}}-{{partition}}-{{start_offset}}.gz",
		"file.compression.type":               "gzip",
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
}
