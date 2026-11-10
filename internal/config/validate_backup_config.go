package config

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

var (
	s3BucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	s3Prefix     = regexp.MustCompile(`^([A-Za-z0-9._-]+/)*$`)
	s3Region     = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
)

// ValidateBackupConfig accepts no backup setting, or a complete one: an http(s) endpoint, an S3 bucket name, a region
// and an optional prefix of path segments ending in "/". Backups also need Kafka Connect.
func ValidateBackupConfig(backup BackupConfig, connectURL string) error {
	if backup == (BackupConfig{}) {
		return nil
	}
	endpoint, err := url.Parse(backup.S3Endpoint)
	switch {
	case err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "":
		return errors.New("backup.s3endpoint must be an http or https URL without credentials, query or fragment")
	case !s3BucketName.MatchString(backup.Bucket):
		return errors.New("backup.bucket must be a valid S3 bucket name")
	case !s3Region.MatchString(backup.Region):
		return errors.New("backup.region must be an S3 region such as us-east-1")
	case !s3Prefix.MatchString(backup.Prefix) || strings.Contains("/"+backup.Prefix, "/./") || strings.Contains("/"+backup.Prefix, "/../"):
		return errors.New("backup.prefix must be empty or path segments ending in /")
	case connectURL == "":
		return errors.New("backups need connect.url")
	}
	return nil
}
