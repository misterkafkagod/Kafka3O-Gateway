package backup

import "testing"

func TestParseBackupObjectName(t *testing.T) {
	topic, partition, offset, ok := ParseBackupObjectName("kafka-a/a-1-b/a-1-b-3-00000000000000000007.gz")
	if !ok || topic != "a-1-b" || partition != 3 || offset != 7 {
		t.Fatalf("%q %d %d %t", topic, partition, offset, ok)
	}
	for _, key := range []string{"x/orders-1-00000000000000000035.zst", "x/orders-1-00000000000000000035.snappy", "x/orders-1-00000000000000000035"} {
		if topic, partition, offset, ok := ParseBackupObjectName(key); !ok || topic != "orders" || partition != 1 || offset != 35 {
			t.Errorf("%q: %q %d %d %t", key, topic, partition, offset, ok)
		}
	}
	for _, key := range []string{"x/orders-0-1.gz", "x/orders-0-00000000000000000000.gz.tmp", "x/orders-0-00000000000000000000.lz4", "x/notes.txt"} {
		if _, _, _, ok := ParseBackupObjectName(key); ok {
			t.Errorf("%q parsed", key)
		}
	}
}
