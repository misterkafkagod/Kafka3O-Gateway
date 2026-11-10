package backup

import "testing"

func TestBackupConnectorName(t *testing.T) {
	if got := BackupConnectorName("orders.v2"); got != "kafka3o-backup-orders.v2" {
		t.Fatalf("name = %q", got)
	}
}
