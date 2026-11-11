package backup

import "testing"

func TestReadBackupOptions(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]string
		want   BackupOptions
	}{
		{"M13 configuration", BackupConnectorConfig(settings, "orders"), BackupOptions{StartFrom: "earliest", Compression: "gzip"}},
		{"no keys", map[string]string{}, BackupOptions{StartFrom: "earliest", Compression: "gzip"}},
		{"every key", map[string]string{"consumer.override.auto.offset.reset": "latest", "file.compression.type": "snappy", "file.max.records": "500"},
			BackupOptions{StartFrom: "latest", Compression: "snappy", RecordsPerFile: 500}},
		{"none compression", map[string]string{"file.compression.type": "none"}, BackupOptions{StartFrom: "earliest", Compression: "none"}},
		{"unparsable record limit", map[string]string{"file.max.records": "lots"}, BackupOptions{StartFrom: "earliest", Compression: "gzip"}},
		{"zero record limit", map[string]string{"file.max.records": "0"}, BackupOptions{StartFrom: "earliest", Compression: "gzip"}},
	}
	for _, c := range cases {
		if got := ReadBackupOptions(c.config); got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}
