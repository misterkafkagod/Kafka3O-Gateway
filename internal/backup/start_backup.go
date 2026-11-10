package backup

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
)

var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

type StartBackup interface {
	Execute(ctx context.Context, topic string) (Backup, error)
}

type startBackup struct {
	settings Settings
	create   connect.CreateConnector
	timeout  time.Duration
}

func NewStartBackup(settings Settings, create connect.CreateConnector, timeout time.Duration) StartBackup {
	return startBackup{settings: settings, create: create, timeout: timeout}
}

// Execute creates the topic's backup connector; a topic has at most one backup.
func (s startBackup) Execute(ctx context.Context, topic string) (Backup, error) {
	if s.settings.S3Endpoint == "" {
		return Backup{}, ErrBackupsNotConfigured
	}
	if !kafkaTopicName.MatchString(topic) || topic == "." || topic == ".." {
		return Backup{}, ErrInvalidTopic
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	name := BackupConnectorName(topic)
	if err := s.create.Execute(ctx, name, BackupConnectorConfig(s.settings, topic)); err != nil {
		if errors.Is(err, connect.ErrConnectorExists) {
			return Backup{}, ErrBackupExists
		}
		return Backup{}, err
	}
	return Backup{Topic: topic, Connector: name, Location: BackupLocation(s.settings, topic)}, nil
}
