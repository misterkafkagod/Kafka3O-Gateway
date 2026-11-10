package backup

import "testing"

func TestBackupLocation(t *testing.T) {
	if got := BackupLocation(Settings{Bucket: "kafka3o-backups", Prefix: "kafka-a/"}, "orders"); got != "s3://kafka3o-backups/kafka-a/orders/" {
		t.Fatalf("location = %q", got)
	}
}
