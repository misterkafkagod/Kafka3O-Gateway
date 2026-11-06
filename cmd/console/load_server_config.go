package main

import (
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/session"
)

func loadServerConfig(getenv func(string) string) (serverConfig, error) {
	config := serverConfig{
		databasePath: getenv("KAFKA3O_CONSOLE_DB_PATH"),
		listen:       getenv("KAFKA3O_CONSOLE_LISTEN"),
		origin:       getenv("KAFKA3O_CONSOLE_ORIGIN"),
		certificate:  getenv("KAFKA3O_CONSOLE_TLS_CERT_FILE"),
		key:          getenv("KAFKA3O_CONSOLE_TLS_KEY_FILE"),
	}
	if config.listen == "" {
		config.listen = "127.0.0.1:8443"
	}
	host, port, err := net.SplitHostPort(config.listen)
	if err != nil {
		return serverConfig{}, fmt.Errorf("%w: listen must be a numeric loopback address and port", errInvalidServerConfig)
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsLoopback() || address.Zone() != "" {
		return serverConfig{}, fmt.Errorf("%w: listen must use a numeric loopback address", errInvalidServerConfig)
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return serverConfig{}, fmt.Errorf("%w: listen port must be between 1 and 65535", errInvalidServerConfig)
	}
	if _, err := session.NewCSRFVerifier(config.origin); err != nil {
		return serverConfig{}, fmt.Errorf("%w: an exact HTTPS origin is required", errInvalidServerConfig)
	}
	for _, path := range []string{config.databasePath, config.certificate, config.key} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return serverConfig{}, fmt.Errorf("%w: database and TLS paths must be absolute", errInvalidServerConfig)
		}
	}
	return config, nil
}
