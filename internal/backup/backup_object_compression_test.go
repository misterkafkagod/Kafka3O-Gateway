package backup

import "testing"

func TestBackupObjectCompression(t *testing.T) {
	for key, want := range map[string]string{
		"p/orders-0-00000000000000000000.gz":     "gzip",
		"p/orders-0-00000000000000000000.zst":    "zstd",
		"p/orders-0-00000000000000000000.snappy": "snappy",
		"p/orders-0-00000000000000000000":        "none",
		"p.gz/orders-0-00000000000000000000":     "none",
	} {
		if got := BackupObjectCompression(key); got != want {
			t.Errorf("%s: %s, want %s", key, got, want)
		}
	}
}
