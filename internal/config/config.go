package config

import "time"

// Config is the gateway configuration. Tags are lowercase so that
// KAFKA3O_* environment overrides (lowercased) match camelCase YAML keys.
type Config struct {
	HTTP     HTTPConfig     `koanf:"http"`
	Kafka    KafkaConfig    `koanf:"kafka"`
	Security SecurityConfig `koanf:"security"`
	Connect  ConnectConfig  `koanf:"connect"`
	Backup   BackupConfig   `koanf:"backup"`
}

// BackupConfig optionally names the S3-compatible location for topic backups made through Kafka Connect; an empty
// endpoint means backups are not configured. Credentials are not here: Connect reads them from its own environment.
type BackupConfig struct {
	S3Endpoint string `koanf:"s3endpoint"`
	Bucket     string `koanf:"bucket"`
	Region     string `koanf:"region"`
	Prefix     string `koanf:"prefix"`
	// The Gateway's read-only listing credentials come only from KAFKA3O_BACKUP_S3_ACCESS_KEY and _SECRET_KEY.
	ReadAccessKey string `koanf:"-"`
	ReadSecretKey string `koanf:"-"`
}

// ConnectConfig optionally points the Gateway at its cluster's Kafka Connect REST API; empty means no Connect.
type ConnectConfig struct {
	URL string `koanf:"url"`
	// OffsetsTopic is the Connect worker's offset.storage.topic, read for restore progress.
	OffsetsTopic string `koanf:"offsetstopic"`
}

// SecurityConfig selects the legacy shared-token mode (default) or the secured mode, in which only the Console
// may call the Gateway and every request needs a current Console delegation.
type SecurityConfig struct {
	Mode                  string `koanf:"mode"`
	ClusterID             string `koanf:"clusterid"`
	ServiceCAFile         string `koanf:"servicecafile"`
	ClientCertificateFile string `koanf:"clientcertificatefile"`
	ClientKeyFile         string `koanf:"clientkeyfile"`
	PolicyURL             string `koanf:"policyurl"`
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
