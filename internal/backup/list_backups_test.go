package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

type fakeConnectors struct {
	connectors []connect.Connector
	err        error
}

func (f fakeConnectors) Execute(context.Context) ([]connect.Connector, error) {
	return f.connectors, f.err
}

type fakeObjects struct {
	byPrefix map[string][]s3store.Object
	err      error
}

func (f fakeObjects) Execute(_ context.Context, prefix string) ([]s3store.Object, error) {
	return f.byPrefix[prefix], f.err
}

func TestListBackupsCombinesConnectAndStorage(t *testing.T) {
	first := time.Date(2026, 11, 10, 7, 35, 0, 0, time.UTC)
	last := time.Date(2026, 11, 10, 8, 1, 0, 0, time.UTC)
	connectors := fakeConnectors{connectors: []connect.Connector{
		{Name: "kafka3o-backup-orders", Type: "sink", State: "RUNNING", Tasks: []connect.Task{{ID: 0, State: "RUNNING"}}},
		{Name: "kafka3o-backup-payments", Type: "sink", State: "RUNNING", Tasks: []connect.Task{{ID: 0, State: "FAILED", Error: "ConnectException: bucket gone"}}},
		{Name: "kafka3o-verify-running", Type: "sink", State: "RUNNING"},
		{Name: "kafka3o-backup-not-a-sink", Type: "source", State: "RUNNING"},
	}}
	objects := fakeObjects{byPrefix: map[string][]s3store.Object{
		"kafka-a/orders/": {{Key: "a", Size: 265, LastModified: first}, {Key: "b", Size: 120, LastModified: last}},
	}}
	backups, err := NewListBackups(settings, connectors, objects, nil, nil).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 2 {
		t.Fatalf("backups = %+v", backups)
	}
	orders, payments := backups[0], backups[1]
	if orders.Topic != "orders" || orders.State != "RUNNING" || orders.Location != "s3://kafka3o-backups/kafka-a/orders/" ||
		orders.Storage == nil || orders.Storage.Objects != 2 || orders.Storage.StoredBytes != 385 || !orders.Storage.LastWrite.Equal(last) {
		t.Fatalf("orders = %+v %+v", orders, orders.Storage)
	}
	if payments.State != "FAILED" || payments.Error != "ConnectException: bucket gone" || payments.Storage == nil || payments.Storage.Objects != 0 || payments.Storage.LastWrite != nil {
		t.Fatalf("payments = %+v %+v", payments, payments.Storage)
	}
}

func TestListBackupsWithoutStorageOrConnect(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{{Name: "kafka3o-backup-orders", Type: "sink", State: "PAUSED"}}}
	backups, err := NewListBackups(settings, connectors, fakeObjects{err: s3store.ErrNoCredentials}, nil, nil).Execute(context.Background())
	if err != nil || len(backups) != 1 || backups[0].Storage != nil || backups[0].State != "PAUSED" {
		t.Fatalf("backups = %+v, %v", backups, err)
	}
	if _, err := NewListBackups(Settings{}, connectors, fakeObjects{}, nil, nil).Execute(context.Background()); !errors.Is(err, ErrBackupsNotConfigured) {
		t.Fatalf("not configured: %v", err)
	}
	if _, err := NewListBackups(settings, fakeConnectors{err: connect.ErrConnectUnreachable}, fakeObjects{}, nil, nil).Execute(context.Background()); !errors.Is(err, connect.ErrConnectUnreachable) {
		t.Fatalf("connect down: %v", err)
	}
}

type fakeCheckRestore struct {
	status RestoreStatus
	topics []string
}

func (f *fakeCheckRestore) Execute(_ context.Context, topic string, _ connect.Connector) RestoreStatus {
	f.topics = append(f.topics, topic)
	return f.status
}

func TestListBackupsIncludesRestores(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{
		{Name: "kafka3o-backup-orders", Type: "sink", State: "PAUSED"},
		{Name: "kafka3o-restore-orders", Type: "source", State: "RUNNING"},
		{Name: "kafka3o-restore-audit", Type: "source", State: "RUNNING"},
	}}
	check := &fakeCheckRestore{status: RestoreStatus{RestoreID: "r-1", State: "RUNNING", Restored: 3, Total: 10}}
	backups, err := NewListBackups(settings, connectors, fakeObjects{}, check, nil).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 2 || backups[0].Topic != "audit" || backups[0].State != "NO JOB" || backups[1].Topic != "orders" ||
		backups[1].Restore == nil || backups[1].Restore.Restored != 3 || len(check.topics) != 2 {
		t.Fatalf("backups = %+v, checked %v", backups, check.topics)
	}
	check.status.Finished = true
	backups, _ = NewListBackups(settings, connectors, fakeObjects{}, check, nil).Execute(context.Background())
	if backups[1].State != "RUNNING" || !backups[1].Restore.Finished {
		t.Fatalf("finished restore = %+v", backups[1])
	}
}

type fakeConfigsByName map[string]map[string]string

func (f fakeConfigsByName) Execute(_ context.Context, name string) (map[string]string, error) {
	if config, ok := f[name]; ok {
		return config, nil
	}
	return nil, connect.ErrConnectorNotFound
}

func TestListBackupsReadsEffectiveSettings(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{
		{Name: "kafka3o-backup-orders", Type: "sink", State: "RUNNING"},
		{Name: "kafka3o-backup-payments", Type: "sink", State: "PAUSED"},
		{Name: "kafka3o-restore-gone", Type: "source", State: "RUNNING"},
	}}
	configs := fakeConfigsByName{"kafka3o-backup-orders": {"file.compression.type": "zstd", "consumer.override.auto.offset.reset": "latest", "file.max.records": "100"}}
	backups, err := NewListBackups(settings, connectors, fakeObjects{}, nil, configs).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 3 {
		t.Fatalf("backups = %+v", backups)
	}
	byTopic := map[string]Backup{}
	for _, backup := range backups {
		byTopic[backup.Topic] = backup
	}
	if got := byTopic["orders"].Settings; got == nil || *got != (BackupOptions{StartFrom: "latest", Compression: "zstd", RecordsPerFile: 100}) {
		t.Fatalf("orders settings = %+v", got)
	}
	if byTopic["payments"].Settings != nil {
		t.Fatalf("an unreadable configuration must leave settings out: %+v", byTopic["payments"].Settings)
	}
	if gone := byTopic["gone"]; gone.State != "NO JOB" || gone.Settings != nil {
		t.Fatalf("a restore without a backup job has no settings: %+v", gone)
	}
}

func TestListBackupsFindsRestoresByTheirOwnNames(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{
		{Name: "kafka3o-backup-orders", Type: "sink", State: "PAUSED"},
		{Name: "kafka3o-restore-orders~r-20261111T033128Z-bb9b5f2e", Type: "source", State: "RUNNING"},
	}}
	check := &fakeCheckRestore{status: RestoreStatus{RestoreID: "r-20261111T033128Z-bb9b5f2e", State: "RUNNING", Restored: 1, Total: 3}}
	backups, err := NewListBackups(settings, connectors, fakeObjects{}, check, nil).Execute(context.Background())
	if err != nil || len(backups) != 1 || backups[0].Restore == nil || backups[0].Restore.Total != 3 || len(check.topics) != 1 || check.topics[0] != "orders" {
		t.Fatalf("%+v %v %v", backups, err, check.topics)
	}
}

func TestListBackupsFlagsRestoresWithoutCheckingThem(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{
		{Name: "kafka3o-backup-orders", Type: "sink", State: "PAUSED"},
		{Name: "kafka3o-backup-payments", Type: "sink", State: "RUNNING"},
		{Name: "kafka3o-restore-orders~r-1", Type: "source", State: "RUNNING"},
	}}
	backups, err := NewListBackups(settings, connectors, fakeObjects{}, nil, nil).Execute(context.Background())
	if err != nil || len(backups) != 2 || !backups[0].Restoring || backups[0].Restore != nil || backups[1].Restoring {
		t.Fatalf("%+v %v", backups, err)
	}
}
