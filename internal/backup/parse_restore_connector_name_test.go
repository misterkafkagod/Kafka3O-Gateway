package backup

import "testing"

func TestParseRestoreConnectorName(t *testing.T) {
	for name, want := range map[string]string{
		"kafka3o-restore-orders~r-20261111T033128Z-bb9b5f2e": "orders",
		"kafka3o-restore-a-1-b~r-1":                          "a-1-b",
		"kafka3o-restore-orders":                             "orders",
	} {
		if topic, ok := ParseRestoreConnectorName(name); !ok || topic != want {
			t.Errorf("%s: %q %t", name, topic, ok)
		}
	}
	for _, name := range []string{"kafka3o-restore-", "kafka3o-restore-~r-1", "kafka3o-restore-orders~", "kafka3o-backup-orders", "orders"} {
		if topic, ok := ParseRestoreConnectorName(name); ok {
			t.Errorf("%s parsed as %q", name, topic)
		}
	}
}
