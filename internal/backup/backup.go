package backup

import "errors"

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
}

var (
	ErrBackupsNotConfigured = errors.New("backups are not configured for this cluster")
	ErrBackupExists         = errors.New("the topic already has a backup")
	ErrInvalidTopic         = errors.New("the topic name is invalid")
)
