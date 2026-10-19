package config

import "time"

// Config is the gateway configuration. Tags are lowercase so that
// KAFKA3O_* environment overrides (lowercased) match camelCase YAML keys.
type Config struct {
	HTTP  HTTPConfig  `koanf:"http"`
	Kafka KafkaConfig `koanf:"kafka"`
}

type HTTPConfig struct {
	Listen string     `koanf:"listen"`
	CORS   CORSConfig `koanf:"cors"`
}

type CORSConfig struct {
	AllowedOrigins []string `koanf:"allowedorigins"`
}

type KafkaConfig struct {
	Brokers []string      `koanf:"brokers"`
	Timeout time.Duration `koanf:"timeout"`
}
