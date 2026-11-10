package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig_AppliesDefaults(t *testing.T) {
	cfg, err := LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		HTTP:    HTTPConfig{Listen: ":8080"},
		Kafka:   KafkaConfig{Brokers: []string{"localhost:9092"}, Timeout: 3 * time.Second},
		Connect: ConnectConfig{OffsetsTopic: "kafka3o-connect-offsets"},
	}
	if diff := cmp.Diff(want, cfg); diff != "" {
		t.Errorf("config mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadConfig_ReadsYAML(t *testing.T) {
	cfg, err := LoadConfig(writeYAML(t, `
http:
  listen: ":9000"
  cors:
    allowedOrigins: ["http://localhost:5173"]
kafka:
  brokers: ["a:9092", "b:9092"]
  timeout: 2s
`))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		HTTP:    HTTPConfig{Listen: ":9000", CORS: CORSConfig{AllowedOrigins: []string{"http://localhost:5173"}}},
		Kafka:   KafkaConfig{Brokers: []string{"a:9092", "b:9092"}, Timeout: 2 * time.Second},
		Connect: ConnectConfig{OffsetsTopic: "kafka3o-connect-offsets"},
	}
	if diff := cmp.Diff(want, cfg); diff != "" {
		t.Errorf("config mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadConfig_EnvOverridesYAML(t *testing.T) {
	path := writeYAML(t, "kafka:\n  brokers: [\"a:9092\"]\n  timeout: 2s\n")
	t.Setenv("KAFKA3O_KAFKA__TIMEOUT", "7s")
	t.Setenv("KAFKA3O_KAFKA__BROKERS", "x:9092,y:9092")

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.Timeout != 7*time.Second {
		t.Errorf("timeout = %v, want 7s", cfg.Kafka.Timeout)
	}
	if diff := cmp.Diff([]string{"x:9092", "y:9092"}, cfg.Kafka.Brokers); diff != "" {
		t.Errorf("brokers mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadConfig_ReadsTLSPaths(t *testing.T) {
	path := writeYAML(t, "kafka:\n  brokers: [\"a:9092\"]\nhttp:\n  tls:\n    certificateFile: cert.pem\n    keyFile: key.pem\n")
	t.Setenv("KAFKA3O_HTTP__TLS__KEYFILE", "override.pem")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.TLS.CertificateFile != "cert.pem" || cfg.HTTP.TLS.KeyFile != "override.pem" {
		t.Errorf("TLS config = %#v", cfg.HTTP.TLS)
	}
}

func TestLoadConfig_ReadsKafkaSecurity(t *testing.T) {
	const password = "synthetic-test-password"
	t.Setenv("KAFKA3O_KAFKA_SASL_PASSWORD", password)
	t.Setenv("KAFKA3O_KAFKA__TLS__CAFILE", "override-ca.pem")
	path := writeYAML(t, `
kafka:
  brokers: ["a:9092"]
  tls:
    enabled: true
    caFile: ca.pem
  sasl:
    mechanism: SCRAM-SHA-512
    username: test-user
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Kafka.TLS.Enabled || cfg.Kafka.TLS.CAFile != "override-ca.pem" ||
		cfg.Kafka.SASL.Mechanism != "SCRAM-SHA-512" || cfg.Kafka.SASL.Username != "test-user" ||
		cfg.Kafka.SASL.Password != password {
		t.Fatal("Kafka security settings were not loaded")
	}
}

func TestLoadConfig_RejectsKafkaPasswordInConfig(t *testing.T) {
	for _, key := range []string{"password", "PASSWORD"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv("KAFKA3O_KAFKA_SASL_PASSWORD", "")
			path := writeYAML(t, "kafka:\n  brokers: [\"a:9092\"]\n  sasl:\n    "+key+": synthetic-test-password\n")
			if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "KAFKA3O_KAFKA_SASL_PASSWORD") {
				t.Fatal("expected an explicit environment-only password error")
			} else if strings.Contains(err.Error(), "synthetic-test-password") {
				t.Fatal("configuration error exposed a password")
			}
		})
	}
	t.Run("nested environment override", func(t *testing.T) {
		t.Setenv("KAFKA3O_KAFKA__SASL__PASSWORD", "synthetic-test-password")
		path := writeYAML(t, "kafka:\n  brokers: [\"a:9092\"]\n")
		if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "KAFKA3O_KAFKA_SASL_PASSWORD") {
			t.Fatal("expected an explicit environment-only password error")
		}
	})
}

func TestLoadConfig_EmptyBrokersIsAnError(t *testing.T) {
	if _, err := LoadConfig(writeYAML(t, "http:\n  listen: \":8080\"\n")); err == nil {
		t.Fatal("expected an error for empty kafka.brokers")
	}
}

func TestLoadConfig_MissingFileIsAnError(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestLoadConfig_ReadsOptionalConnectURL(t *testing.T) {
	t.Setenv("KAFKA3O_CONNECT__URL", "http://kafka-connect.kafka3o-connect-test.svc.cluster.local:8083")
	cfg, err := LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Connect.URL != "http://kafka-connect.kafka3o-connect-test.svc.cluster.local:8083" {
		t.Fatalf("connect url = %q", cfg.Connect.URL)
	}
	t.Setenv("KAFKA3O_CONNECT__URL", "kafka-connect:8083")
	if _, err := LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\n")); err == nil {
		t.Fatal("a Connect URL without a scheme was accepted")
	}
}

func TestLoadConfig_ReadsOptionalBackupSettings(t *testing.T) {
	t.Setenv("KAFKA3O_CONNECT__URL", "http://kafka-connect:8083")
	t.Setenv("KAFKA3O_BACKUP__S3ENDPOINT", "http://seaweedfs-s3:8333")
	t.Setenv("KAFKA3O_BACKUP__BUCKET", "kafka3o-backups")
	t.Setenv("KAFKA3O_BACKUP__REGION", "us-east-1")
	t.Setenv("KAFKA3O_BACKUP__PREFIX", "kafka-a/")
	cfg, err := LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backup != (BackupConfig{S3Endpoint: "http://seaweedfs-s3:8333", Bucket: "kafka3o-backups", Region: "us-east-1", Prefix: "kafka-a/"}) {
		t.Fatalf("backup = %+v", cfg.Backup)
	}
	t.Setenv("KAFKA3O_BACKUP_S3_ACCESS_KEY", "read-key")
	t.Setenv("KAFKA3O_BACKUP_S3_SECRET_KEY", "read-secret")
	cfg, err = LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\n"))
	if err != nil || cfg.Backup.ReadAccessKey != "read-key" || cfg.Backup.ReadSecretKey != "read-secret" {
		t.Fatalf("read credentials = %q/%q, %v", cfg.Backup.ReadAccessKey, cfg.Backup.ReadSecretKey, err)
	}
	if _, err := LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\nbackup:\n  secretkey: x\n")); err == nil {
		t.Fatal("a backup secret in the config file was accepted")
	}
}

func TestLoadConfig_ReadsTheOptionalMetricsListener(t *testing.T) {
	yamlPath := writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\n")
	cfg, err := LoadConfig(yamlPath)
	if err != nil || cfg.Metrics != (MetricsConfig{}) {
		t.Fatalf("metrics = %+v, %v", cfg.Metrics, err)
	}
	t.Setenv("KAFKA3O_METRICS__LISTEN", ":9464")
	if _, err := LoadConfig(yamlPath); err == nil || !strings.Contains(err.Error(), "KAFKA3O_METRICS_TOKEN") {
		t.Fatalf("a metrics listener without a token was accepted: %v", err)
	}
	t.Setenv("KAFKA3O_METRICS_TOKEN", "short")
	if _, err := LoadConfig(yamlPath); err == nil {
		t.Fatal("a short metrics token was accepted")
	}
	token := strings.Repeat("m", 32)
	t.Setenv("KAFKA3O_METRICS_TOKEN", token)
	cfg, err = LoadConfig(yamlPath)
	if err != nil || cfg.Metrics != (MetricsConfig{Listen: ":9464", Token: token}) {
		t.Fatalf("metrics = %+v, %v", cfg.Metrics, err)
	}
	if _, err := LoadConfig(writeYAML(t, "kafka:\n  brokers: [\"localhost:9092\"]\nmetrics:\n  token: x\n")); err == nil {
		t.Fatal("a metrics token in the config file was accepted")
	}
}
