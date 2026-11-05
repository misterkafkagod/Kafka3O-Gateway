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
	TLS    TLSConfig  `koanf:"tls"`
}

type TLSConfig struct {
	CertificateFile string `koanf:"certificatefile"`
	KeyFile         string `koanf:"keyfile"`
}

type CORSConfig struct {
	AllowedOrigins []string `koanf:"allowedorigins"`
}

type KafkaConfig struct {
	Brokers []string        `koanf:"brokers"`
	Timeout time.Duration   `koanf:"timeout"`
	TLS     KafkaTLSConfig  `koanf:"tls"`
	SASL    KafkaSASLConfig `koanf:"sasl"`
}

type KafkaTLSConfig struct {
	Enabled bool   `koanf:"enabled"`
	CAFile  string `koanf:"cafile"`
}

type KafkaSASLConfig struct {
	Mechanism string `koanf:"mechanism"`
	Username  string `koanf:"username"`
	Password  string `koanf:"-"`
}
