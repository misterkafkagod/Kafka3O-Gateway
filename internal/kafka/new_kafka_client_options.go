package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/config"
)

func newKafkaClientOptions(cfg config.KafkaConfig, readFile func(string) ([]byte, error)) ([]kgo.Opt, error) {
	if err := config.ValidateKafkaSecurity(cfg); err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.TLS.Enabled {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.TLS.CAFile != "" {
			pem, err := readFile(cfg.TLS.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read Kafka TLS CA file: %w", err)
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			if !tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
				return nil, errors.New("Kafka TLS CA file does not contain a valid certificate")
			}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	switch strings.ToUpper(strings.TrimSpace(cfg.SASL.Mechanism)) {
	case "PLAIN":
		auth := plain.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}
		opts = append(opts, kgo.SASL(auth.AsMechanism()))
	case "SCRAM-SHA-256":
		auth := scram.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}
		opts = append(opts, kgo.SASL(auth.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		auth := scram.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}
		opts = append(opts, kgo.SASL(auth.AsSha512Mechanism()))
	}
	return opts, nil
}
