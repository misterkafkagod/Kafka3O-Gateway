package backup

import (
	"context"
	"errors"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

// ManageBackup pauses, resumes or deletes a topic's backup connector; deleting never touches the stored objects.
type ManageBackup interface {
	Execute(ctx context.Context, topic, kind string) error
}

type manageBackup struct {
	settings Settings
	manage   connect.ManageConnector
}

func NewManageBackup(settings Settings, manage connect.ManageConnector) ManageBackup {
	return manageBackup{settings: settings, manage: manage}
}

func (m manageBackup) Execute(ctx context.Context, topic, kind string) error {
	if m.settings.S3Endpoint == "" {
		return ErrBackupsNotConfigured
	}
	if !kafkaTopicName.MatchString(topic) || topic == "." || topic == ".." {
		return ErrInvalidTopic
	}
	if kind != connect.ActionPause && kind != connect.ActionResume && kind != connect.ActionDelete {
		return connect.ErrInvalidConnectAction
	}
	err := m.manage.Execute(ctx, connect.Action{Kind: kind, Connector: BackupConnectorName(topic)})
	if errors.Is(err, connect.ErrConnectorNotFound) {
		return ErrBackupNotFound
	}
	return err
}
