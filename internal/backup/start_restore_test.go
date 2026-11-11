package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

func restoreFixture(backupState string, createErr error) (*fakeManage, *fakeCreate, StartRestore) {
	connectors := fakeConnectors{}
	if backupState != "" {
		connectors.connectors = []connect.Connector{{Name: "kafka3o-backup-orders", Type: "sink", State: backupState}}
	}
	objects := fakeObjects{byPrefix: map[string][]s3store.Object{"kafka-a/orders/": {
		{Key: "kafka-a/orders/orders-0-00000000000000000000.gz"}, {Key: "kafka-a/orders/orders-1-00000000000000000004.gz"},
	}}}
	manage := &fakeManage{}
	create := &fakeCreate{err: createErr}
	return manage, create, NewStartRestore(settings, connectors, fakeConnectorConfig{config: BackupConnectorConfig(settings, "orders", BackupOptions{StartFrom: "earliest", Compression: "gzip"})},
		objects, manage, create, func() string { return "r-42" }, time.Second)
}

func TestStartRestorePausesTheBackupAndCreatesTheRestore(t *testing.T) {
	manage, create, start := restoreFixture("RUNNING", nil)
	restore, err := start.Execute(context.Background(), "orders", RestoreFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if restore != (Restore{Topic: "orders", RestoreID: "r-42", Objects: 2, Skipped: 0}) {
		t.Fatalf("restore = %+v", restore)
	}
	if len(manage.actions) != 1 || manage.actions[0] != (connect.Action{Kind: "pause", Connector: "kafka3o-backup-orders"}) {
		t.Fatalf("actions = %+v", manage.actions)
	}
	if create.name != "kafka3o-restore-orders~r-42" || create.config["transforms.unwrap.snapshot.boundary"] != "0:0,1:4" ||
		create.config["transforms.unwrap.resume.backup"] != "true" || create.config["tasks.max"] != "2" ||
		create.config["file.compression.type"] != "gzip" || create.config["file.name.template"] != "{{topic}}-{{partition}}-{{start_offset}}.gz" {
		t.Fatalf("created %s %v", create.name, create.config)
	}
}

func TestStartRestoreLeavesAPausedOrMissingBackupAlone(t *testing.T) {
	for _, state := range []string{"PAUSED", ""} {
		manage, create, start := restoreFixture(state, nil)
		if _, err := start.Execute(context.Background(), "orders", RestoreFilter{}); err != nil {
			t.Fatalf("%q: %v", state, err)
		}
		if len(manage.actions) != 0 || create.config["transforms.unwrap.resume.backup"] != "false" {
			t.Fatalf("%q: actions %+v, resume %s", state, manage.actions, create.config["transforms.unwrap.resume.backup"])
		}
	}
}

func TestStartRestoreRefusals(t *testing.T) {
	inProgress := fakeConnectors{connectors: []connect.Connector{{Name: "kafka3o-restore-orders", Type: "source", State: "RUNNING"}}}
	manage, create := &fakeManage{}, &fakeCreate{}
	start := NewStartRestore(settings, inProgress, fakeConnectorConfig{}, fakeObjects{}, manage, create, func() string { return "r" }, time.Second)
	if _, err := start.Execute(context.Background(), "orders", RestoreFilter{}); !errors.Is(err, ErrRestoreInProgress) || create.name != "" {
		t.Errorf("in progress: %v", err)
	}
	empty := NewStartRestore(settings, fakeConnectors{}, fakeConnectorConfig{}, fakeObjects{}, manage, create, func() string { return "r" }, time.Second)
	if _, err := empty.Execute(context.Background(), "orders", RestoreFilter{}); !errors.Is(err, ErrNothingToRestore) {
		t.Errorf("nothing stored: %v", err)
	}
	noStorage := NewStartRestore(settings, fakeConnectors{}, fakeConnectorConfig{}, fakeObjects{err: s3store.ErrNoCredentials}, manage, create, func() string { return "r" }, time.Second)
	if _, err := noStorage.Execute(context.Background(), "orders", RestoreFilter{}); !errors.Is(err, ErrStorageUnavailable) {
		t.Errorf("no storage access: %v", err)
	}
	if _, err := NewStartRestore(Settings{}, fakeConnectors{}, fakeConnectorConfig{}, fakeObjects{}, manage, create, func() string { return "r" }, time.Second).Execute(context.Background(), "orders", RestoreFilter{}); !errors.Is(err, ErrBackupsNotConfigured) {
		t.Errorf("not configured: %v", err)
	}
	if len(manage.actions) != 0 {
		t.Fatalf("refusals paused something: %+v", manage.actions)
	}
}

func TestStartRestoreResumesTheBackupWhenCreationFails(t *testing.T) {
	manage, _, start := restoreFixture("RUNNING", connect.ErrConnectRejected)
	if _, err := start.Execute(context.Background(), "orders", RestoreFilter{}); !errors.Is(err, connect.ErrConnectRejected) {
		t.Fatal(err)
	}
	if len(manage.actions) != 2 || manage.actions[1].Kind != "resume" {
		t.Fatalf("actions = %+v", manage.actions)
	}
	manage, _, start = restoreFixture("RUNNING", connect.ErrConnectActionUnknown)
	if _, err := start.Execute(context.Background(), "orders", RestoreFilter{}); !errors.Is(err, connect.ErrConnectActionUnknown) {
		t.Fatal(err)
	}
	if len(manage.actions) != 1 {
		t.Fatalf("an uncertain restore resumed the backup: %+v", manage.actions)
	}
}

var mixedObjects = fakeObjects{byPrefix: map[string][]s3store.Object{"kafka-a/orders/": {
	{Key: "kafka-a/orders/orders-0-00000000000000000000.gz", LastModified: time.Date(2026, 11, 10, 7, 0, 0, 0, time.UTC)},
	{Key: "kafka-a/orders/orders-0-00000000000000000012.zst", LastModified: time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC)},
	{Key: "kafka-a/orders/orders-1-00000000000000000003.zst", LastModified: time.Date(2026, 11, 11, 2, 0, 1, 0, time.UTC)},
}}}

func TestStartRestoreReadsTheBackupJobsCompression(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{{Name: "kafka3o-backup-orders", Type: "sink", State: "RUNNING"}}}
	config := fakeConnectorConfig{config: BackupConnectorConfig(settings, "orders", BackupOptions{StartFrom: "latest", Compression: "zstd"})}
	create := &fakeCreate{}
	restore, err := NewStartRestore(settings, connectors, config, mixedObjects, &fakeManage{}, create, func() string { return "r" }, time.Second).Execute(context.Background(), "orders", RestoreFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if restore.Objects != 2 || restore.Skipped != 1 || create.config["file.compression.type"] != "zstd" ||
		create.config["file.name.template"] != "{{topic}}-{{partition}}-{{start_offset}}.zst" || create.config["transforms.unwrap.snapshot.boundary"] != "0:12,1:3" {
		t.Fatalf("restore %+v, config %v", restore, create.config)
	}
}

func TestStartRestoreWithoutAJobUsesTheNewestObjectsCompression(t *testing.T) {
	create := &fakeCreate{}
	restore, err := NewStartRestore(settings, fakeConnectors{}, fakeConnectorConfig{err: connect.ErrConnectorNotFound}, mixedObjects, &fakeManage{}, create,
		func() string { return "r" }, time.Second).Execute(context.Background(), "orders", RestoreFilter{})
	if err != nil || restore.Objects != 2 || restore.Skipped != 1 || create.config["file.compression.type"] != "zstd" {
		t.Fatalf("restore %+v, %v, config %v", restore, err, create.config["file.compression.type"])
	}
}

func TestStartRestoreStopsWhenTheJobsSettingsCannotBeRead(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{{Name: "kafka3o-backup-orders", Type: "sink", State: "RUNNING"}}}
	manage, create := &fakeManage{}, &fakeCreate{}
	_, err := NewStartRestore(settings, connectors, fakeConnectorConfig{err: connect.ErrConnectUnreachable}, mixedObjects, manage, create,
		func() string { return "r" }, time.Second).Execute(context.Background(), "orders", RestoreFilter{})
	if !errors.Is(err, connect.ErrConnectUnreachable) || len(manage.actions) != 0 || create.name != "" {
		t.Fatalf("err %v, actions %+v, created %q", err, manage.actions, create.name)
	}
}

func TestStartRestoreKeepsOnlyTheChosenPartitionsAndWindow(t *testing.T) {
	_, create, start := restoreFixture("RUNNING", nil)
	from := time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	restore, err := start.Execute(context.Background(), "orders", RestoreFilter{From: &from, To: &to, Partitions: []int{1}})
	if err != nil {
		t.Fatal(err)
	}
	if restore.Objects != 1 || create.config["transforms.unwrap.snapshot.boundary"] != "1:4" || create.config["tasks.max"] != "1" ||
		create.config["transforms.unwrap.time.from"] != "1794358800000" || create.config["transforms.unwrap.time.to"] != "1794362400000" {
		t.Fatalf("restore %+v, config %v", restore, create.config)
	}
}

func TestStartRestoreRefusesFiltersBeforeConnect(t *testing.T) {
	from := time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)
	before := from.Add(-time.Minute)
	for name, filter := range map[string]RestoreFilter{
		"reversed window":   {From: &from, To: &before},
		"no partitions":     {Partitions: []int{}},
		"unknown partition": {Partitions: []int{0, 7}},
	} {
		manage, create, start := restoreFixture("RUNNING", nil)
		_, err := start.Execute(context.Background(), "orders", filter)
		if !errors.Is(err, ErrInvalidRestoreFilter) || len(manage.actions) != 0 || create.name != "" {
			t.Errorf("%s: err %v, actions %+v, created %q", name, err, manage.actions, create.name)
		}
	}
}

func TestStartRestoreSeesARunningRestoreUnderItsOwnName(t *testing.T) {
	connectors := fakeConnectors{connectors: []connect.Connector{
		{Name: "kafka3o-restore-orders-archive~r-1", Type: "source", State: "RUNNING"},
		{Name: "kafka3o-restore-orders~r-20261111T033128Z-bb9b5f2e", Type: "source", State: "RUNNING"},
	}}
	create := &fakeCreate{}
	_, err := NewStartRestore(settings, connectors, fakeConnectorConfig{}, mixedObjects, &fakeManage{}, create, func() string { return "r" }, time.Second).Execute(context.Background(), "orders", RestoreFilter{})
	if !errors.Is(err, ErrRestoreInProgress) || create.name != "" {
		t.Fatalf("err %v, created %q", err, create.name)
	}
	other := fakeConnectors{connectors: connectors.connectors[:1]}
	if _, err := NewStartRestore(settings, other, fakeConnectorConfig{err: connect.ErrConnectorNotFound}, mixedObjects, &fakeManage{}, create, func() string { return "r-2" }, time.Second).
		Execute(context.Background(), "orders", RestoreFilter{}); err != nil || create.name != "kafka3o-restore-orders~r-2" {
		t.Fatalf("another topic's restore blocked this one: %v %q", err, create.name)
	}
}
