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

type fakeCounts map[string]int64

func (f fakeCounts) Execute(_ context.Context, object s3store.Object) (int64, error) {
	count, ok := f[object.Key]
	if !ok {
		return 0, ErrStorageUnavailable
	}
	return count, nil
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
