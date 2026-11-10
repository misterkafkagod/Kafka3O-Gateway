package config

import "testing"

func TestValidateBackupConfig(t *testing.T) {
	valid := BackupConfig{S3Endpoint: "http://seaweedfs-s3.kafka3o-backup-storage.svc.cluster.local:8333", Bucket: "kafka3o-backups", Region: "us-east-1", Prefix: "kafka-a/"}
	if err := ValidateBackupConfig(BackupConfig{}, ""); err != nil {
		t.Fatalf("no backups: %v", err)
	}
	if err := ValidateBackupConfig(valid, "http://connect:8083"); err != nil {
		t.Fatalf("valid: %v", err)
	}
	for name, invalid := range map[string]BackupConfig{
		"endpoint":       {S3Endpoint: "seaweedfs:8333", Bucket: valid.Bucket, Region: valid.Region},
		"credentials":    {S3Endpoint: "http://a:b@s3:8333", Bucket: valid.Bucket, Region: valid.Region},
		"bucket":         {S3Endpoint: valid.S3Endpoint, Bucket: "Bad_Bucket", Region: valid.Region},
		"region":         {S3Endpoint: valid.S3Endpoint, Bucket: valid.Bucket, Region: "US East"},
		"prefix slash":   {S3Endpoint: valid.S3Endpoint, Bucket: valid.Bucket, Region: valid.Region, Prefix: "kafka-a"},
		"prefix escape":  {S3Endpoint: valid.S3Endpoint, Bucket: valid.Bucket, Region: valid.Region, Prefix: "../x/"},
		"partial config": {Bucket: valid.Bucket},
	} {
		if err := ValidateBackupConfig(invalid, "http://connect:8083"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if err := ValidateBackupConfig(valid, ""); err == nil {
		t.Error("backups without Connect were accepted")
	}
}
