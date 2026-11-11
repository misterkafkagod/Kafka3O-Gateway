package backup

import (
	"errors"
	"time"
)

// Settings is the cluster's S3-compatible backup location; Kafka Connect holds the credentials.
type Settings struct {
	S3Endpoint string
	Bucket     string
	Region     string
	Prefix     string
}

// Backup is one topic's continuous backup job.
type Backup struct {
	Topic     string `json:"topic"`
	Connector string `json:"connector"`
	Location  string `json:"location"`
	// State and Error come from Kafka Connect; Storage is nil when the backup storage could not be listed.
	State   string          `json:"state,omitempty"`
	Error   string          `json:"error,omitempty"`
	Storage *StorageSummary `json:"storage,omitempty"`
	Restore *RestoreStatus  `json:"restore,omitempty"`
	// Settings are the job's effective settings, read back from Connect; nil when they could not be read.
	Settings *BackupOptions `json:"settings,omitempty"`
}

// BackupOptions are the user-chosen backup settings; anything else in the connector configuration is generated.
type BackupOptions struct {
	StartFrom      string `json:"startFrom"`
	Compression    string `json:"compression"`
	RecordsPerFile int    `json:"recordsPerFile"`
}

// RestoreStatus is a running or just-finished restore of the topic's backup.
type RestoreStatus struct {
	RestoreID string `json:"restoreId"`
	State     string `json:"state"`
	Restored  int64  `json:"restored"`
	Total     int64  `json:"total"`
	Finished  bool   `json:"finished"`
	Error     string `json:"error,omitempty"`
}

// StorageSummary describes what a backup has written so far.
type StorageSummary struct {
	Objects     int        `json:"objects"`
	StoredBytes int64      `json:"storedBytes"`
	LastWrite   *time.Time `json:"lastWrite"`
}

var (
	ErrBackupsNotConfigured = errors.New("backups are not configured for this cluster")
	ErrBackupExists         = errors.New("the topic already has a backup")
	ErrInvalidTopic         = errors.New("the topic name is invalid")
	ErrBackupNotFound       = errors.New("the topic has no backup")
	ErrRestoreInProgress    = errors.New("a restore of this topic is already running")
	ErrNothingToRestore     = errors.New("the topic's backup holds no objects")
	ErrStorageUnavailable   = errors.New("the backup storage cannot be listed")
)

// Restore is a restore started for a topic from the objects that existed at that moment.
type Restore struct {
	Topic     string `json:"topic"`
	RestoreID string `json:"restoreId"`
	Objects   int    `json:"objects"`
}
