package kafka

import "testing"

func TestApplyConnectOffsetRecordKeepsTheLatestCountPerObject(t *testing.T) {
	counts := map[string]int64{}
	object := `{"bucket":"kafka3o-backups","objectKey":"kafka-a/orders/orders-0-00000000000000000000.gz"}`
	ApplyConnectOffsetRecord(counts, "kafka3o-restore-orders", []byte(`["kafka3o-restore-orders",`+object+`]`), []byte(`{"recordCount":5}`))
	ApplyConnectOffsetRecord(counts, "kafka3o-restore-orders", []byte(`["kafka3o-restore-orders",`+object+`]`), []byte(`{"recordCount":10}`))
	ApplyConnectOffsetRecord(counts, "kafka3o-restore-orders", []byte(`["kafka3o-restore-orders",{"objectKey":"other.gz"}]`), []byte(`{"recordCount":1}`))
	ApplyConnectOffsetRecord(counts, "kafka3o-restore-orders", []byte(`["kafka3o-restore-payments",`+object+`]`), []byte(`{"recordCount":99}`))
	ApplyConnectOffsetRecord(counts, "kafka3o-restore-orders", []byte(`not json`), []byte(`{"recordCount":7}`))
	if len(counts) != 2 || counts["kafka-a/orders/orders-0-00000000000000000000.gz"] != 10 || counts["other.gz"] != 1 {
		t.Fatalf("counts = %v", counts)
	}
	ApplyConnectOffsetRecord(counts, "kafka3o-restore-orders", []byte(`["kafka3o-restore-orders",{"objectKey":"other.gz"}]`), nil)
	if _, ok := counts["other.gz"]; ok {
		t.Fatal("a tombstone did not remove the object")
	}
}
