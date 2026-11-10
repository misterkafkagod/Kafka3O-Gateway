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
)
