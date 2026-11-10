package backup

import "testing"

func TestRestoreConnectorConfig(t *testing.T) {
	config := RestoreConnectorConfig(settings, "orders", "r-1", "0:20,2:5", 3, true)
	for key, want := range map[string]string{
		"connector.class":                     "io.aiven.kafka.connect.s3.source.S3SourceConnector",
		"topic":                               "orders",
		"aws.s3.prefix":                       "kafka-a/orders/",
		"tasks.max":                           "3",
		"distribution.type":                   "partition",
		"file.name.template":                  "{{topic}}-{{partition}}-{{start_offset}}.gz",
		"transforms.unwrap.type":              "dev.kafka3o.connect.UnwrapBackupRecord",
		"transforms.unwrap.restore.id":        "r-1",
		"transforms.unwrap.snapshot.boundary": "0:20,2:5",
		"transforms.unwrap.resume.backup":     "true",
		"aws.secret.access.key":               "${env:BACKUP_S3_SECRET_KEY}",
	} {
		if config[key] != want {
			t.Errorf("%s = %q, want %q", key, config[key], want)
		}
	}
	if RestoreConnectorConfig(settings, "orders", "r", "0:1", 40, false)["tasks.max"] != "8" || RestoreConnectorConfig(settings, "orders", "r", "", 0, false)["tasks.max"] != "1" {
		t.Fatal("tasks.max is not bounded to 1..8")
	}
}
