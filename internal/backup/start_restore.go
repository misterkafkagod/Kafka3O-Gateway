package backup

import (
	"context"
	"errors"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

type StartRestore interface {
	Execute(ctx context.Context, topic string) (Restore, error)
}

type startRestore struct {
	settings   Settings
	connectors connect.ListConnectors
	objects    s3store.ListObjects
	manage     connect.ManageConnector
	create     connect.CreateConnector
	restoreID  func() string
	timeout    time.Duration
}

func NewStartRestore(settings Settings, connectors connect.ListConnectors, objects s3store.ListObjects, manage connect.ManageConnector,
	create connect.CreateConnector, restoreID func() string, timeout time.Duration) StartRestore {
	return startRestore{settings: settings, connectors: connectors, objects: objects, manage: manage, create: create, restoreID: restoreID, timeout: timeout}
}

// Execute fixes the restore's snapshot to the objects that exist now, pauses the topic's running backup so it cannot
// add objects or re-save restored records, and creates the restore connector.
func (s startRestore) Execute(ctx context.Context, topic string) (Restore, error) {
	if s.settings.S3Endpoint == "" {
		return Restore{}, ErrBackupsNotConfigured
	}
	if !kafkaTopicName.MatchString(topic) || topic == "." || topic == ".." {
		return Restore{}, ErrInvalidTopic
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	connectors, err := s.connectors.Execute(ctx)
	if err != nil {
		return Restore{}, err
	}
	resumeBackup := false
	for _, connector := range connectors {
		switch connector.Name {
		case RestoreConnectorName(topic):
			return Restore{}, ErrRestoreInProgress
		case BackupConnectorName(topic):
			resumeBackup = connector.State != "PAUSED"
		}
	}
	objects, err := s.objects.Execute(ctx, s.settings.Prefix+topic+"/")
	if err != nil {
		return Restore{}, errors.Join(ErrStorageUnavailable, err)
	}
	boundary, count, partitions := SnapshotBoundary(objects, topic)
	if count == 0 {
		return Restore{}, ErrNothingToRestore
	}
	if resumeBackup {
		if err := s.manage.Execute(ctx, connect.Action{Kind: connect.ActionPause, Connector: BackupConnectorName(topic)}); err != nil {
			return Restore{}, err
		}
	}
	id := s.restoreID()
	if err := s.create.Execute(ctx, RestoreConnectorName(topic), RestoreConnectorConfig(s.settings, topic, id, boundary, partitions, resumeBackup)); err != nil {
		if resumeBackup && !errors.Is(err, connect.ErrConnectActionUnknown) {
			// Nothing was restored, so the backup goes back to running.
			_ = s.manage.Execute(ctx, connect.Action{Kind: connect.ActionResume, Connector: BackupConnectorName(topic)})
		}
		if errors.Is(err, connect.ErrConnectorExists) {
			return Restore{}, ErrRestoreInProgress
		}
		return Restore{}, err
	}
	return Restore{Topic: topic, RestoreID: id, Objects: count}, nil
}
