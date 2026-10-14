// Package config loads gateway settings from environment variables.
package config

import (
	"errors"
	"strings"
)

const defaultListen = ":8080"

// Config holds the gateway's start-up settings.
type Config struct {
	Brokers []string // KAFKA3O_BROKERS, required, comma-separated
	Listen  string   // KAFKA3O_LISTEN, default ":8080"
	UIDir   string   // KAFKA3O_UI_DIR, optional
}

// ErrNoBrokers is returned when KAFKA3O_BROKERS is missing or empty.
var ErrNoBrokers = errors.New("KAFKA3O_BROKERS is required (comma-separated host:port list)")

// Load reads the configuration through getenv, so tests can supply their own lookup.
func Load(getenv func(string) string) (Config, error) {
	var brokers []string
	for _, b := range strings.Split(getenv("KAFKA3O_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return Config{}, ErrNoBrokers
	}

	listen := strings.TrimSpace(getenv("KAFKA3O_LISTEN"))
	if listen == "" {
		listen = defaultListen
	}

	return Config{
		Brokers: brokers,
		Listen:  listen,
		UIDir:   strings.TrimSpace(getenv("KAFKA3O_UI_DIR")),
	}, nil
}
