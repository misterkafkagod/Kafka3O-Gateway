package s3store

import (
	"errors"
	"time"
)

// Credentials is an S3 access key pair; the Gateway's pair is read-only.
type Credentials struct {
	AccessKey string
	SecretKey string
}

type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

var (
	ErrNoCredentials = errors.New("no S3 credentials are configured")
	ErrUnavailable   = errors.New("the S3 storage is unavailable")
)
