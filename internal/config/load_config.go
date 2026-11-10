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
	if err := ValidateKafkaSecurity(cfg.Kafka); err != nil {
		return Config{}, err
	}
	if err := ValidateConnectURL(cfg.Connect.URL); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
