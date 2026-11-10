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
	backups, err := NewListBackups(settings, connectors, objects).Execute(context.Background())
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
	backups, err := NewListBackups(settings, connectors, fakeObjects{err: s3store.ErrNoCredentials}).Execute(context.Background())
	if err != nil || len(backups) != 1 || backups[0].Storage != nil || backups[0].State != "PAUSED" {
		t.Fatalf("backups = %+v, %v", backups, err)
	}
	if _, err := NewListBackups(Settings{}, connectors, fakeObjects{}).Execute(context.Background()); !errors.Is(err, ErrBackupsNotConfigured) {
		t.Fatalf("not configured: %v", err)
	}
	if _, err := NewListBackups(settings, fakeConnectors{err: connect.ErrConnectUnreachable}, fakeObjects{}).Execute(context.Background()); !errors.Is(err, connect.ErrConnectUnreachable) {
		t.Fatalf("connect down: %v", err)
	}
}
