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
	return manage, create, NewStartRestore(settings, connectors, objects, manage, create, func() string { return "r-42" }, time.Second)
}

func TestStartRestorePausesTheBackupAndCreatesTheRestore(t *testing.T) {
	manage, create, start := restoreFixture("RUNNING", nil)
	restore, err := start.Execute(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if restore != (Restore{Topic: "orders", RestoreID: "r-42", Objects: 2}) {
		t.Fatalf("restore = %+v", restore)
	}
	if len(manage.actions) != 1 || manage.actions[0] != (connect.Action{Kind: "pause", Connector: "kafka3o-backup-orders"}) {
		t.Fatalf("actions = %+v", manage.actions)
	}
	if create.name != "kafka3o-restore-orders" || create.config["transforms.unwrap.snapshot.boundary"] != "0:0,1:4" ||
		create.config["transforms.unwrap.resume.backup"] != "true" || create.config["tasks.max"] != "2" {
		t.Fatalf("created %s %v", create.name, create.config)
	}
}

func TestStartRestoreLeavesAPausedOrMissingBackupAlone(t *testing.T) {
	for _, state := range []string{"PAUSED", ""} {
		manage, create, start := restoreFixture(state, nil)
		if _, err := start.Execute(context.Background(), "orders"); err != nil {
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
	start := NewStartRestore(settings, inProgress, fakeObjects{}, manage, create, func() string { return "r" }, time.Second)
	if _, err := start.Execute(context.Background(), "orders"); !errors.Is(err, ErrRestoreInProgress) || create.name != "" {
		t.Errorf("in progress: %v", err)
	}
	empty := NewStartRestore(settings, fakeConnectors{}, fakeObjects{}, manage, create, func() string { return "r" }, time.Second)
	if _, err := empty.Execute(context.Background(), "orders"); !errors.Is(err, ErrNothingToRestore) {
		t.Errorf("nothing stored: %v", err)
	}
	noStorage := NewStartRestore(settings, fakeConnectors{}, fakeObjects{err: s3store.ErrNoCredentials}, manage, create, func() string { return "r" }, time.Second)
	if _, err := noStorage.Execute(context.Background(), "orders"); !errors.Is(err, ErrStorageUnavailable) {
		t.Errorf("no storage access: %v", err)
	}
	if _, err := NewStartRestore(Settings{}, fakeConnectors{}, fakeObjects{}, manage, create, func() string { return "r" }, time.Second).Execute(context.Background(), "orders"); !errors.Is(err, ErrBackupsNotConfigured) {
		t.Errorf("not configured: %v", err)
	}
	if len(manage.actions) != 0 {
		t.Fatalf("refusals paused something: %+v", manage.actions)
	}
}

func TestStartRestoreResumesTheBackupWhenCreationFails(t *testing.T) {
	manage, _, start := restoreFixture("RUNNING", connect.ErrConnectRejected)
	if _, err := start.Execute(context.Background(), "orders"); !errors.Is(err, connect.ErrConnectRejected) {
		t.Fatal(err)
	}
	if len(manage.actions) != 2 || manage.actions[1].Kind != "resume" {
		t.Fatalf("actions = %+v", manage.actions)
	}
	manage, _, start = restoreFixture("RUNNING", connect.ErrConnectActionUnknown)
	if _, err := start.Execute(context.Background(), "orders"); !errors.Is(err, connect.ErrConnectActionUnknown) {
		t.Fatal(err)
	}
	if len(manage.actions) != 1 {
		t.Fatalf("an uncertain restore resumed the backup: %+v", manage.actions)
	}
}
