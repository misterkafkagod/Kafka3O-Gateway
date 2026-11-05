package config

import (
	"errors"
	"strings"
)

func ValidateKafkaSecurity(cfg KafkaConfig) error {
	if !cfg.TLS.Enabled && cfg.TLS.CAFile != "" {
		return errors.New("kafka.tls.caFile requires kafka.tls.enabled")
	}
	mechanism := strings.TrimSpace(cfg.SASL.Mechanism)
	if cfg.SASL.Mechanism != "" && mechanism == "" {
		return errors.New("kafka.sasl.mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
	}
	switch strings.ToUpper(mechanism) {
	case "":
		if cfg.SASL.Username != "" || cfg.SASL.Password != "" {
			return errors.New("Kafka SASL credentials require kafka.sasl.mechanism")
		}
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if cfg.SASL.Username == "" || cfg.SASL.Password == "" {
			return errors.New("Kafka SASL requires kafka.sasl.username and KAFKA3O_KAFKA_SASL_PASSWORD")
		}
	default:
		return errors.New("kafka.sasl.mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
	}
	return nil
}
