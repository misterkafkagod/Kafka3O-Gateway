package backup

const byteArrayConverter = "org.apache.kafka.connect.converters.ByteArrayConverter"

// BackupConnectorConfig is the s3-backup-connectors recipe's sink configuration: lossless gzip jsonl of key, value,
// timestamp and headers, skipping records a restore wrote (header kafka3o-restore). Credentials stay in Connect's
// environment and are only referenced here.
func BackupConnectorConfig(settings Settings, topic string) map[string]string {
	return map[string]string{
		"connector.class":                     "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
		"tasks.max":                           "1",
		"topics":                              topic,
		"aws.access.key.id":                   "${env:BACKUP_S3_ACCESS_KEY}",
		"aws.secret.access.key":               "${env:BACKUP_S3_SECRET_KEY}",
		"aws.s3.endpoint":                     settings.S3Endpoint,
		"aws.s3.region":                       settings.Region,
		"aws.s3.bucket.name":                  settings.Bucket,
		"aws.s3.prefix":                       settings.Prefix + topic + "/",
		"file.name.template":                  "{{topic}}-{{partition}}-{{start_offset}}",
		"file.compression.type":               "gzip",
		"format.output.type":                  "jsonl",
		"format.output.fields":                "key,value,timestamp,headers",
		"format.output.fields.value.encoding": "base64",
		"key.converter":                       byteArrayConverter,
		"value.converter":                     byteArrayConverter,
		"header.converter":                    byteArrayConverter,
		"consumer.override.auto.offset.reset": "earliest",
		"transforms":                          "skipRestored",
		"transforms.skipRestored.type":        "org.apache.kafka.connect.transforms.Filter",
		"transforms.skipRestored.predicate":   "restored",
		"predicates":                          "restored",
		"predicates.restored.type":            "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
		"predicates.restored.name":            "kafka3o-restore",
	}
}
