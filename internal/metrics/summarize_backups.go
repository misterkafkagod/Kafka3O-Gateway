package metrics

import "github.com/misterkafkagod/Kafka3O-Gateway/internal/backup"

// SummarizeBackups keeps what tells whether each backup is healthy; the list arrives sorted by topic.
func SummarizeBackups(backups []backup.Backup) Backups {
	summary := Backups{Backups: make([]BackupHealth, 0, len(backups))}
	for _, item := range backups {
		health := BackupHealth{Topic: item.Topic, State: item.State, Error: item.Error, Restoring: item.Restoring}
		if item.Storage != nil {
			objects, stored := item.Storage.Objects, item.Storage.StoredBytes
			health.StorageListed, health.Objects, health.StoredBytes, health.LastWrite = true, &objects, &stored, item.Storage.LastWrite
		}
		summary.Backups = append(summary.Backups, health)
	}
	return summary
}
