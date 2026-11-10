package backup

import "testing"

func TestParseSnapshotBoundary(t *testing.T) {
	highest, ok := ParseSnapshotBoundary("0:20, 2:5")
	if !ok || len(highest) != 2 || highest[0] != 20 || highest[2] != 5 {
		t.Fatalf("%v %t", highest, ok)
	}
	for _, invalid := range []string{"", "0", "a:1", "0:x"} {
		if _, ok := ParseSnapshotBoundary(invalid); ok {
			t.Errorf("%q parsed", invalid)
		}
	}
}
