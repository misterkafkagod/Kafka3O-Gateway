package metrics

import (
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"
)

func TestSummarizeBackups(t *testing.T) {
	written := time.Date(2026, 11, 11, 3, 30, 30, 0, time.UTC)
	summary := SummarizeBackups([]backup.Backup{
		{Topic: "orders", State: "RUNNING", Storage: &backup.StorageSummary{Objects: 10, StoredBytes: 1900, LastWrite: &written}},
		{Topic: "empty", State: "RUNNING", Storage: &backup.StorageSummary{}},
		{Topic: "unlisted", State: "PAUSED", Restoring: true},
		{Topic: "failed", State: "FAILED", Error: "ConnectException: bucket gone"},
	})
	orders := summary.Backups[0]
	if !orders.StorageListed || *orders.Objects != 10 || *orders.StoredBytes != 1900 || !orders.LastWrite.Equal(written) || orders.Restoring {
		t.Fatalf("orders = %+v", orders)
	}
	if empty := summary.Backups[1]; !empty.StorageListed || *empty.Objects != 0 || empty.LastWrite != nil {
		t.Fatalf("empty = %+v", empty)
	}
	if unlisted := summary.Backups[2]; unlisted.StorageListed || unlisted.Objects != nil || unlisted.StoredBytes != nil || !unlisted.Restoring {
		t.Fatalf("unlisted = %+v", unlisted)
	}
	if failed := summary.Backups[3]; failed.State != "FAILED" || failed.Error != "ConnectException: bucket gone" {
		t.Fatalf("failed = %+v", failed)
	}
}
