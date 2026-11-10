package backup

import "testing"

func TestRestoreConnectorName(t *testing.T) {
	if got := RestoreConnectorName("orders"); got != "kafka3o-restore-orders" {
		t.Fatalf("name = %q", got)
	}
}
