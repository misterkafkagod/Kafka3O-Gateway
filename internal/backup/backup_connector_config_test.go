package backup

import (
	"strings"
	"testing"
)

func TestBackupConnectorConfigFollowsTheDiscoveryRecipe(t *testing.T) {
	config := BackupConnectorConfig(Settings{S3Endpoint: "http://s3:8333", Bucket: "kafka3o-backups", Region: "us-east-1", Prefix: "kafka-a/"}, "orders", BackupOptions{StartFrom: "earliest", Compression: "gzip"})
	for key, want := range map[string]string{
		"connector.class":                     "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
		"topics":                              "orders",
		"aws.s3.prefix":                       "kafka-a/orders/",
		"aws.s3.endpoint":                     "http://s3:8333",
		"format.output.type":                  "jsonl",
		"format.output.fields":                "key,value,timestamp,headers",
		"format.output.fields.value.encoding": "base64",
		"value.converter":                     "org.apache.kafka.connect.converters.ByteArrayConverter",
		"predicates.restored.name":            "kafka3o-restore",
		"aws.secret.access.key":               "${env:BACKUP_S3_SECRET_KEY}",
	} {
		if config[key] != want {
			t.Errorf("%s = %q, want %q", key, config[key], want)
		}
	}
	for key, value := range config {
		if strings.Contains(strings.ToLower(key), "key.id") || strings.Contains(strings.ToLower(key), "secret") {
			if !strings.HasPrefix(value, "${env:") {
				t.Errorf("%s holds a literal credential", key)
			}
		}
	}
}

func TestBackupConnectorConfigDefaultsAreTheM13Configuration(t *testing.T) {
	config := BackupConnectorConfig(settings, "orders", BackupOptions{StartFrom: "earliest", Compression: "gzip"})
	if config["file.compression.type"] != "gzip" || config["consumer.override.auto.offset.reset"] != "earliest" || len(config) != 24 {
		t.Fatalf("config = %v (%d keys)", config, len(config))
	}
	if _, ok := config["file.max.records"]; ok {
		t.Fatal("no record limit by default")
	}
}

func TestBackupConnectorConfigMapsTheOptions(t *testing.T) {
	config := BackupConnectorConfig(settings, "orders", BackupOptions{StartFrom: "latest", Compression: "zstd", RecordsPerFile: 100})
	if config["file.compression.type"] != "zstd" || config["consumer.override.auto.offset.reset"] != "latest" || config["file.max.records"] != "100" {
		t.Fatalf("config = %v", config)
	}
	if got := ReadBackupOptions(config); got != (BackupOptions{StartFrom: "latest", Compression: "zstd", RecordsPerFile: 100}) {
		t.Fatalf("read back = %+v", got)
	}
}
