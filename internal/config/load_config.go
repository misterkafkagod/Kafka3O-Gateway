package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// LoadConfig reads the YAML file at path (skipped when path is empty), applies
// KAFKA3O_* environment overrides ("__" separates nested keys) and validates the result.
func LoadConfig(path string) (Config, error) {
	k := koanf.New(".")
	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return Config{}, fmt.Errorf("load config file %q: %w", path, err)
		}
	}
	envProvider := env.Provider(".", env.Opt{Prefix: envPrefix, TransformFunc: EnvKey})
	if err := k.Load(envProvider, nil); err != nil {
		return Config{}, fmt.Errorf("load environment overrides: %w", err)
	}
	for key := range k.All() {
		if strings.EqualFold(key, "kafka.sasl.password") {
			return Config{}, errors.New("set the Kafka SASL password only with KAFKA3O_KAFKA_SASL_PASSWORD")
		}
		if strings.HasPrefix(strings.ToLower(key), "backup.") && (strings.Contains(strings.ToLower(key), "key") || strings.Contains(strings.ToLower(key), "secret")) {
			return Config{}, errors.New("set backup S3 credentials only with KAFKA3O_BACKUP_S3_ACCESS_KEY and KAFKA3O_BACKUP_S3_SECRET_KEY")
		}
		if strings.EqualFold(key, "metrics.token") {
			return Config{}, errors.New("set the metrics token only with KAFKA3O_METRICS_TOKEN")
		}
	}

	cfg := Config{
		HTTP:  HTTPConfig{Listen: ":8080"},
		Kafka: KafkaConfig{Timeout: 3 * time.Second}, // below the UI's 5 s gateway timeout
	}
	err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{
		Tag: "koanf",
		DecoderConfig: &mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
			),
			WeaklyTypedInput: true,
			Result:           &cfg,
		},
	})
	if err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if len(cfg.Kafka.Brokers) == 0 {
		return Config{}, errors.New("kafka.brokers must list at least one broker")
	}
	cfg.Kafka.SASL.Password = os.Getenv("KAFKA3O_KAFKA_SASL_PASSWORD")
	cfg.Backup.ReadAccessKey = os.Getenv("KAFKA3O_BACKUP_S3_ACCESS_KEY")
	cfg.Backup.ReadSecretKey = os.Getenv("KAFKA3O_BACKUP_S3_SECRET_KEY")
	cfg.Metrics.Token = os.Getenv("KAFKA3O_METRICS_TOKEN")
	if cfg.Metrics.Listen != "" && (len(cfg.Metrics.Token) < 32 || !bearerTokenPattern.MatchString(cfg.Metrics.Token)) {
		return Config{}, errors.New("metrics.listen needs KAFKA3O_METRICS_TOKEN with at least 32 bearer-token characters")
	}
	if err := ValidateKafkaSecurity(cfg.Kafka); err != nil {
		return Config{}, err
	}
	if cfg.Connect.OffsetsTopic == "" {
		cfg.Connect.OffsetsTopic = "kafka3o-connect-offsets"
	}
	if err := ValidateConnectURL(cfg.Connect.URL); err != nil {
		return Config{}, err
	}
	if err := ValidateBackupConfig(cfg.Backup, cfg.Connect.URL); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
