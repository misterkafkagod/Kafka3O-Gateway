package backup

import (
	"context"
	"slices"
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
	restores   CheckRestore
	configs    connect.FetchConnectorConfig
}

func NewListBackups(settings Settings, connectors connect.ListConnectors, objects s3store.ListObjects, restores CheckRestore, configs connect.FetchConnectorConfig) ListBackups {
	return listBackups{settings: settings, connectors: connectors, objects: objects, restores: restores, configs: configs}
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
	restoring := map[string]connect.Connector{}
	backedUp := map[string]bool{}
	for _, connector := range connectors {
		if topic, ok := ParseRestoreConnectorName(connector.Name); ok && connector.Type == "source" {
			restoring[topic] = connector
		}
		if strings.HasPrefix(connector.Name, prefix) && connector.Type == "sink" {
			backedUp[strings.TrimPrefix(connector.Name, prefix)] = true
		}
	}
	// A restore whose backup job was deleted still appears, so it can be followed to the end.
	for topic := range restoring {
		if !backedUp[topic] {
			connectors = append(connectors, connect.Connector{Name: BackupConnectorName(topic), Type: "sink", State: "NO JOB"})
		}
	}
	slices.SortFunc(connectors, connect.CompareConnectors)
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
		if connector.State != "NO JOB" && l.configs != nil {
			if config, err := l.configs.Execute(ctx, connector.Name); err == nil {
				options := ReadBackupOptions(config)
				backup.Settings = &options
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
		_, backup.Restoring = restoring[topic]
		if restore, ok := restoring[topic]; ok && l.restores != nil {
			status := l.restores.Execute(ctx, topic, restore)
			backup.Restore = &status
			if status.Finished && backup.State == "PAUSED" {
				backup.State = "RUNNING"
			}
		}
		backups = append(backups, backup)
	}
	return backups, nil
}
