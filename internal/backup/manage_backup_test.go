package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

type fakeManage struct {
	err     error
	actions []connect.Action
}

func (f *fakeManage) Execute(_ context.Context, action connect.Action) error {
	f.actions = append(f.actions, action)
	return f.err
}

func TestManageBackupSendsTheActionToTheTopicsConnector(t *testing.T) {
	manage := &fakeManage{}
	for _, kind := range []string{connect.ActionPause, connect.ActionResume, connect.ActionDelete} {
		if err := NewManageBackup(settings, manage).Execute(context.Background(), "orders", kind); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	for i, kind := range []string{"pause", "resume", "delete"} {
		if manage.actions[i] != (connect.Action{Kind: kind, Connector: "kafka3o-backup-orders"}) {
			t.Fatalf("action %d = %+v", i, manage.actions[i])
		}
	}
}

func TestManageBackupRefusals(t *testing.T) {
	manage := &fakeManage{}
	if err := NewManageBackup(Settings{}, manage).Execute(context.Background(), "orders", "pause"); !errors.Is(err, ErrBackupsNotConfigured) {
		t.Errorf("not configured: %v", err)
	}
	if err := NewManageBackup(settings, manage).Execute(context.Background(), "a/b", "pause"); !errors.Is(err, ErrInvalidTopic) {
		t.Errorf("invalid topic: %v", err)
	}
	if err := NewManageBackup(settings, manage).Execute(context.Background(), "orders", "restart"); !errors.Is(err, connect.ErrInvalidConnectAction) {
		t.Errorf("restart is not a backup action: %v", err)
	}
	if len(manage.actions) != 0 {
		t.Fatalf("refused requests reached Connect: %+v", manage.actions)
	}
	if err := NewManageBackup(settings, &fakeManage{err: connect.ErrConnectorNotFound}).Execute(context.Background(), "orders", "delete"); !errors.Is(err, ErrBackupNotFound) {
		t.Errorf("no backup: %v", err)
	}
}
