package backup

import (
	"context"
	"strings"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/connect"
	"github.com/misterkafkagod/Kafka3O-Gateway/internal/s3store"
)

type ListBackups interface {
	Execute(ctx context.Context) ([]Backup, error)
}

type listBackups struct {
	settings   Settings
	connectors connect.ListConnectors
	objects    s3store.ListObjects
}

func NewListBackups(settings Settings, connectors connect.ListConnectors, objects s3store.ListObjects) ListBackups {
	return listBackups{settings: settings, connectors: connectors, objects: objects}
}

// Execute reports every backup connector with its Connect state and, when the storage can be listed, what it wrote.
func (l listBackups) Execute(ctx context.Context) ([]Backup, error) {
	if l.settings.S3Endpoint == "" {
		return nil, ErrBackupsNotConfigured
	}
	connectors, err := l.connectors.Execute(ctx)
	if err != nil {
		return nil, err
	}
	prefix := BackupConnectorName("")
	backups := []Backup{}
	for _, connector := range connectors {
		if !strings.HasPrefix(connector.Name, prefix) || connector.Type != "sink" {
			continue
		}
		topic := strings.TrimPrefix(connector.Name, prefix)
		backup := Backup{Topic: topic, Connector: connector.Name, Location: BackupLocation(l.settings, topic), State: connector.State, Error: connector.Error}
		for _, task := range connector.Tasks {
			if backup.Error == "" && task.Error != "" {
				backup.Error = task.Error
			}
			if task.State == "FAILED" && backup.State == "RUNNING" {
				backup.State = "FAILED"
			}
		}
		if objects, err := l.objects.Execute(ctx, l.settings.Prefix+topic+"/"); err == nil {
			summary := StorageSummary{Objects: len(objects)}
			for _, object := range objects {
				summary.StoredBytes += object.Size
				if summary.LastWrite == nil || object.LastModified.After(*summary.LastWrite) {
					modified := object.LastModified
					summary.LastWrite = &modified
				}
			}
			backup.Storage = &summary
		}
		backups = append(backups, backup)
	}
	return backups, nil
}
