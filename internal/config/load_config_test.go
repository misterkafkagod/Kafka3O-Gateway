package config

import (
	"os"
	"path/filepath"
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
		HTTP:  HTTPConfig{Listen: ":8080"},
		Kafka: KafkaConfig{Brokers: []string{"localhost:9092"}, Timeout: 3 * time.Second},
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
		HTTP:  HTTPConfig{Listen: ":9000", CORS: CORSConfig{AllowedOrigins: []string{"http://localhost:5173"}}},
		Kafka: KafkaConfig{Brokers: []string{"a:9092", "b:9092"}, Timeout: 2 * time.Second},
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
