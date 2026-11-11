package backup

import "testing"

func TestRestoreConnectorName(t *testing.T) {
	if got := RestoreConnectorName("orders", "r-20261111T033128Z-bb9b5f2e"); got != "kafka3o-restore-orders~r-20261111T033128Z-bb9b5f2e" {
		t.Fatalf("name = %q", got)
	}
}
