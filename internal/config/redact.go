package config

import (
	"encoding/json"
	"log/slog"
)

// masked replaces every sensitive value in logs and dumps (FUNC-SPEC §8.2).
const masked = "***"

// redacted is Config without its methods, so slog and encoding/json render the
// struct directly instead of re-entering LogValue / String.
type redacted Config

// Redacted returns a copy with the SASL password, both TLS private-key paths,
// and every API-key digest masked. Slices are copied so the original is untouched.
func (c Config) Redacted() Config {
	r := c
	if r.Kafka.SASL.Password != "" {
		r.Kafka.SASL.Password = masked
	}
	if r.Kafka.TLS.KeyFile != "" {
		r.Kafka.TLS.KeyFile = masked
	}
	if r.HTTP.TLS.KeyFile != "" {
		r.HTTP.TLS.KeyFile = masked
	}
	r.Auth.Keys = make([]Key, len(c.Auth.Keys))
	for i, k := range c.Auth.Keys {
		k.SHA256 = masked
		r.Auth.Keys[i] = k
	}
	return r
}

// String renders the redacted configuration as compact JSON, so a Config can be
// printed at start-up without ever exposing a secret.
func (c Config) String() string {
	b, err := json.Marshal(redacted(c.Redacted()))
	if err != nil {
		return "config{" + err.Error() + "}"
	}
	return string(b)
}

// LogValue implements slog.LogValuer: every log line that carries a Config
// carries the redacted form.
func (c Config) LogValue() slog.Value {
	return slog.AnyValue(redacted(c.Redacted()))
}
