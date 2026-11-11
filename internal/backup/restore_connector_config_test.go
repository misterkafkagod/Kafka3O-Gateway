package backup

import "testing"

func TestRestoreConnectorConfig(t *testing.T) {
	config := RestoreConnectorConfig(settings, "orders", "gzip", "r-1", "0:20,2:5", 3, true, AllRecords)
	for key, want := range map[string]string{
		"connector.class":                     "io.aiven.kafka.connect.s3.source.S3SourceConnector",
		"topic":                               "orders",
		"aws.s3.prefix":                       "kafka-a/orders/",
		"tasks.max":                           "3",
		"distribution.type":                   "partition",
		"file.name.template":                  "{{topic}}-{{partition}}-{{start_offset}}.gz",
		"file.compression.type":               "gzip",
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
	if RestoreConnectorConfig(settings, "orders", "gzip", "r", "0:1", 40, false, AllRecords)["tasks.max"] != "8" || RestoreConnectorConfig(settings, "orders", "gzip", "r", "", 0, false, AllRecords)["tasks.max"] != "1" {
		t.Fatal("tasks.max is not bounded to 1..8")
	}
}

func TestRestoreConnectorConfigFollowsTheCompression(t *testing.T) {
	for compression, template := range map[string]string{
		"zstd":   "{{topic}}-{{partition}}-{{start_offset}}.zst",
		"snappy": "{{topic}}-{{partition}}-{{start_offset}}.snappy",
		"none":   "{{topic}}-{{partition}}-{{start_offset}}",
	} {
		config := RestoreConnectorConfig(settings, "orders", compression, "r", "0:1", 1, false, AllRecords)
		if config["file.name.template"] != template || config["file.compression.type"] != compression {
			t.Errorf("%s: %q %q", compression, config["file.name.template"], config["file.compression.type"])
		}
	}
}

func TestRestoreConnectorConfigCarriesTheTimeWindow(t *testing.T) {
	if config := RestoreConnectorConfig(settings, "orders", "gzip", "r", "0:1", 1, false, AllRecords); config["transforms.unwrap.time.from"] != "" || config["transforms.unwrap.time.to"] != "" {
		t.Fatalf("no window expected: %v", config)
	}
	config := RestoreConnectorConfig(settings, "orders", "gzip", "r", "0:1", 1, false, RecordWindow{From: 1000, To: 2000})
	if config["transforms.unwrap.time.from"] != "1000" || config["transforms.unwrap.time.to"] != "2000" {
		t.Fatalf("window: %v", config)
	}
	onlyFrom := RestoreConnectorConfig(settings, "orders", "gzip", "r", "0:1", 1, false, RecordWindow{From: 1000, To: AllRecords.To})
	if _, ok := onlyFrom["transforms.unwrap.time.to"]; ok || onlyFrom["transforms.unwrap.time.from"] != "1000" {
		t.Fatalf("only a start: %v", onlyFrom)
	}
}
