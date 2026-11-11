package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

type fakeConnectorConfig struct {
	config map[string]string
	err    error
}

func (f fakeConnectorConfig) Execute(context.Context, string) (map[string]string, error) {
	return f.config, f.err
}

// fakeCounts holds each object's line count; any window keeps every line.
type fakeCounts map[string]int64

func (f fakeCounts) Execute(_ context.Context, object s3store.Object, _ RecordWindow) (ObjectScan, error) {
	count, ok := f[object.Key]
	if !ok {
		return ObjectScan{}, ErrStorageUnavailable
	}
	return ObjectScan{Matched: count, LastMatch: count}, nil
}

// fakeScans holds each object's scan for a windowed restore.
type fakeScans map[string]ObjectScan

func (f fakeScans) Execute(_ context.Context, object s3store.Object, window RecordWindow) (ObjectScan, error) {
	if window == AllRecords {
		return ObjectScan{}, errors.New("a windowed restore was scanned without its window")
	}
	return f[object.Key], nil
}

type fakeOffsets struct {
	committed map[string]int64
	err       error
}

func (f fakeOffsets) Execute(context.Context, string) (map[string]int64, error) {
	return f.committed, f.err
}

const (
	first  = "kafka-a/orders/orders-0-00000000000000000000.gz"
	second = "kafka-a/orders/orders-0-00000000000000000020.gz"
	later  = "kafka-a/orders/orders-0-00000000000000000030.gz"
)

func restoreCheck(committed map[string]int64, resume string, manage *fakeManage) CheckRestore {
	config := fakeConnectorConfig{config: map[string]string{"transforms.unwrap.restore.id": "r-1",
		"transforms.unwrap.snapshot.boundary": "0:20", "transforms.unwrap.resume.backup": resume}}
	objects := fakeObjects{byPrefix: map[string][]s3store.Object{"kafka-a/orders/": {{Key: first}, {Key: second}, {Key: later}}}}
	return NewCheckRestore(settings, config, objects, fakeCounts{first: 20, second: 1, later: 5}, fakeOffsets{committed: committed}, manage)
}

var runningRestore = connect.Connector{Name: "kafka3o-restore-orders", Type: "source", State: "RUNNING"}

func TestCheckRestoreReportsProgressWithinTheSnapshot(t *testing.T) {
	manage := &fakeManage{}
	status := restoreCheck(map[string]int64{first: 12, later: 5}, "true", manage).Execute(context.Background(), "orders", runningRestore)
	if status != (RestoreStatus{RestoreID: "r-1", State: "RUNNING", Restored: 12, Total: 21}) || len(manage.actions) != 0 {
		t.Fatalf("status = %+v, actions %+v", status, manage.actions)
	}
}

func TestCheckRestoreFinishesAndResumesTheBackup(t *testing.T) {
	manage := &fakeManage{}
	status := restoreCheck(map[string]int64{first: 20, second: 1}, "true", manage).Execute(context.Background(), "orders", runningRestore)
	if !status.Finished || status.Restored != 21 || status.Total != 21 {
		t.Fatalf("status = %+v", status)
	}
	if len(manage.actions) != 2 || manage.actions[0] != (connect.Action{Kind: "delete", Connector: "kafka3o-restore-orders"}) ||
		manage.actions[1] != (connect.Action{Kind: "resume", Connector: "kafka3o-backup-orders"}) {
		t.Fatalf("actions = %+v", manage.actions)
	}
	manage = &fakeManage{}
	restoreCheck(map[string]int64{first: 20, second: 1}, "false", manage).Execute(context.Background(), "orders", runningRestore)
	if len(manage.actions) != 1 || manage.actions[0].Kind != "delete" {
		t.Fatalf("a backup that was paused before the restore was resumed: %+v", manage.actions)
	}
}

func TestCheckRestoreLeavesFailedRestoresAndBackupsAlone(t *testing.T) {
	manage := &fakeManage{}
	failed := connect.Connector{Name: "kafka3o-restore-orders", Type: "source", State: "RUNNING",
		Tasks: []connect.Task{{ID: 0, State: "FAILED", Error: "DataException: malformed backup record"}}}
	status := restoreCheck(map[string]int64{first: 20, second: 1}, "true", manage).Execute(context.Background(), "orders", failed)
	if status.State != "FAILED" || status.Error != "DataException: malformed backup record" || status.Finished || len(manage.actions) != 0 {
		t.Fatalf("status = %+v, actions %+v", status, manage.actions)
	}
	unreadable := NewCheckRestore(settings, fakeConnectorConfig{config: map[string]string{"transforms.unwrap.snapshot.boundary": "0:20"}},
		fakeObjects{}, fakeCounts{}, fakeOffsets{err: errors.New("offsets topic unavailable")}, manage)
	if status := unreadable.Execute(context.Background(), "orders", runningRestore); status.Finished || status.Error == "" {
		t.Fatalf("unreadable offsets = %+v", status)
	}
	if len(manage.actions) != 0 {
		t.Fatalf("actions = %+v", manage.actions)
	}
}

func TestCheckRestoreCountsOnlyTheRestoresCompression(t *testing.T) {
	const zstdFirst, zstdSecond = "kafka-a/orders/orders-0-00000000000000000012.zst", "kafka-a/orders/orders-1-00000000000000000003.zst"
	config := fakeConnectorConfig{config: map[string]string{"transforms.unwrap.restore.id": "r-1", "transforms.unwrap.snapshot.boundary": "0:12,1:3",
		"transforms.unwrap.resume.backup": "false", "file.compression.type": "zstd"}}
	objects := fakeObjects{byPrefix: map[string][]s3store.Object{"kafka-a/orders/": {{Key: first}, {Key: zstdFirst}, {Key: zstdSecond}}}}
	manage := &fakeManage{}
	status := NewCheckRestore(settings, config, objects, fakeCounts{first: 20, zstdFirst: 4, zstdSecond: 2},
		fakeOffsets{committed: map[string]int64{zstdFirst: 4, zstdSecond: 2}}, manage).Execute(context.Background(), "orders", runningRestore)
	if !status.Finished || status.Total != 6 || status.Restored != 6 {
		t.Fatalf("the gzip object of an earlier backup blocked or joined the zstd restore: %+v", status)
	}
}

func windowedCheck(committed map[string]int64, scans fakeScans, manage *fakeManage) CheckRestore {
	config := fakeConnectorConfig{config: map[string]string{"transforms.unwrap.restore.id": "r-1", "transforms.unwrap.snapshot.boundary": "0:20",
		"transforms.unwrap.resume.backup": "false", "transforms.unwrap.time.from": "1000", "transforms.unwrap.time.to": "2000"}}
	objects := fakeObjects{byPrefix: map[string][]s3store.Object{"kafka-a/orders/": {{Key: first}, {Key: second}, {Key: later}}}}
	return NewCheckRestore(settings, config, objects, scans, fakeOffsets{committed: committed}, manage)
}

func TestCheckRestoreWithATimeWindowWaitsForEachObjectsLastMatch(t *testing.T) {
	// first: 3 of its 20 lines match, the last on line 15; second: nothing matches.
	scans := fakeScans{first: {Matched: 3, LastMatch: 15}, second: {}}
	manage := &fakeManage{}
	status := windowedCheck(map[string]int64{first: 14}, scans, manage).Execute(context.Background(), "orders", runningRestore)
	if status.Finished || status.Restored != 0 || status.Total != 3 || len(manage.actions) != 0 {
		t.Fatalf("before the last match: %+v", status)
	}
	status = windowedCheck(map[string]int64{first: 15}, scans, manage).Execute(context.Background(), "orders", runningRestore)
	if !status.Finished || status.Restored != 3 || status.Total != 3 || len(manage.actions) != 1 || manage.actions[0].Kind != "delete" {
		t.Fatalf("after the last match: %+v %+v", status, manage.actions)
	}
}

func TestCheckRestoreWithAWindowMatchingNothingFinishesAtOnce(t *testing.T) {
	manage := &fakeManage{}
	status := windowedCheck(map[string]int64{}, fakeScans{first: {}, second: {}}, manage).Execute(context.Background(), "orders", runningRestore)
	if !status.Finished || status.Total != 0 || len(manage.actions) != 1 {
		t.Fatalf("status %+v, actions %+v", status, manage.actions)
	}
}
