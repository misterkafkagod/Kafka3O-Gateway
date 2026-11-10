package backup

import (
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

func TestSnapshotBoundary(t *testing.T) {
	objects := []s3store.Object{
		{Key: "kafka-a/orders/orders-0-00000000000000000000.gz"},
		{Key: "kafka-a/orders/orders-0-00000000000000000020.gz"},
		{Key: "kafka-a/orders/orders-2-00000000000000000005.gz"},
		{Key: "kafka-a/orders/orders-1-00000000000000000003.gz.tmp"},
		{Key: "kafka-a/orders/other-0-00000000000000000099.gz"},
	}
	boundary, count, partitions := SnapshotBoundary(objects, "orders")
	if boundary != "0:20,2:5" || count != 3 || partitions != 2 {
		t.Fatalf("boundary %q, count %d, partitions %d", boundary, count, partitions)
	}
	if boundary, count, _ := SnapshotBoundary(nil, "orders"); boundary != "" || count != 0 {
		t.Fatalf("empty: %q %d", boundary, count)
	}
	// Topic names may contain dashes and digits.
	if boundary, _, _ := SnapshotBoundary([]s3store.Object{{Key: "p/a-1-b-3-00000000000000000007.gz"}}, "a-1-b"); boundary != "3:7" {
		t.Fatalf("dashed topic: %q", boundary)
	}
}
