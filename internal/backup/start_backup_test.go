package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeCreate struct {
	err    error
	name   string
	config map[string]string
}

func (f *fakeCreate) Execute(_ context.Context, name string, config map[string]string) error {
	f.name, f.config = name, config
	return f.err
}

var settings = Settings{S3Endpoint: "http://s3:8333", Bucket: "kafka3o-backups", Region: "us-east-1", Prefix: "kafka-a/"}

func TestStartBackupCreatesTheTopicsConnector(t *testing.T) {
	create := &fakeCreate{}
	backup, err := NewStartBackup(settings, create, time.Second).Execute(context.Background(), "orders", nil)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Topic != "orders" || backup.Connector != "kafka3o-backup-orders" || backup.Location != "s3://kafka3o-backups/kafka-a/orders/" ||
		backup.Settings == nil || *backup.Settings != (BackupOptions{StartFrom: "earliest", Compression: "gzip"}) ||
		create.name != "kafka3o-backup-orders" || create.config["topics"] != "orders" {
		t.Fatalf("backup %+v, created %s %v", backup, create.name, create.config["topics"])
	}
}

func TestStartBackupRefusals(t *testing.T) {
	if _, err := NewStartBackup(Settings{}, &fakeCreate{}, time.Second).Execute(context.Background(), "orders", nil); !errors.Is(err, ErrBackupsNotConfigured) {
		t.Errorf("not configured: %v", err)
	}
	for _, topic := range []string{"", ".", "..", "a/b", "bad topic"} {
		create := &fakeCreate{}
		if _, err := NewStartBackup(settings, create, time.Second).Execute(context.Background(), topic, nil); !errors.Is(err, ErrInvalidTopic) || create.name != "" {
			t.Errorf("%q: %v", topic, err)
		}
	}
	if _, err := NewStartBackup(settings, &fakeCreate{err: connect.ErrConnectorExists}, time.Second).Execute(context.Background(), "orders", nil); !errors.Is(err, ErrBackupExists) {
		t.Errorf("duplicate: %v", err)
	}
	if _, err := NewStartBackup(settings, &fakeCreate{err: connect.ErrConnectUnreachable}, time.Second).Execute(context.Background(), "orders", nil); !errors.Is(err, connect.ErrConnectUnreachable) {
		t.Errorf("unreachable: %v", err)
	}
}

func TestStartBackupUsesTheChosenOptions(t *testing.T) {
	create := &fakeCreate{}
	backup, err := NewStartBackup(settings, create, time.Second).Execute(context.Background(), "orders", &BackupOptions{StartFrom: "latest", Compression: "snappy", RecordsPerFile: 50})
	if err != nil {
		t.Fatal(err)
	}
	if create.config["file.compression.type"] != "snappy" || create.config["file.max.records"] != "50" || *backup.Settings != (BackupOptions{StartFrom: "latest", Compression: "snappy", RecordsPerFile: 50}) {
		t.Fatalf("config %v, backup %+v", create.config, backup.Settings)
	}
}

func TestStartBackupRefusesInvalidOptionsBeforeConnect(t *testing.T) {
	create := &fakeCreate{}
	if _, err := NewStartBackup(settings, create, time.Second).Execute(context.Background(), "orders", &BackupOptions{Compression: "lz4"}); !errors.Is(err, ErrInvalidBackupSettings) || create.name != "" {
		t.Fatalf("err %v, created %q", err, create.name)
	}
}
