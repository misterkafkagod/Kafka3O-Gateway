package backup

import "testing"

func TestParseBackupObjectName(t *testing.T) {
	topic, partition, offset, ok := ParseBackupObjectName("kafka-a/a-1-b/a-1-b-3-00000000000000000007.gz")
	if !ok || topic != "a-1-b" || partition != 3 || offset != 7 {
		t.Fatalf("%q %d %d %t", topic, partition, offset, ok)
	}
	for _, key := range []string{"x/orders-0-1.gz", "x/orders-0-00000000000000000000.gz.tmp", "x/notes.txt"} {
		if _, _, _, ok := ParseBackupObjectName(key); ok {
			t.Errorf("%q parsed", key)
		}
	}
}
