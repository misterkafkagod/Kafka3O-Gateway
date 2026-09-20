package app

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/config"
)

// mapKeys maps the loaded configuration's API keys onto middleware.Key —
// internal/api/middleware may not import internal/config (TECH-SPEC §5.3).
// config.Validate already proved every SHA256 is 64 hex characters, so a
// decode error here means Run was called with an unvalidated Config.
func mapKeys(keys []config.Key) ([]middleware.Key, error) {
	out := make([]middleware.Key, len(keys))
	for i, k := range keys {
		digest, err := hex.DecodeString(k.SHA256)
		if err != nil || len(digest) != 32 {
			return nil, fmt.Errorf("app: auth.keys[%d].sha256: %q is not a 64-character hex digest", i, k.SHA256)
		}
		out[i] = middleware.Key{ID: k.ID, Tier: k.Tier, SHA256: [32]byte(digest)}
	}
	return out, nil
}

// parseCIDRs parses the configured trusted-proxy CIDRs (TECH-SPEC §6.1 B6).
// config.Validate already proved every entry parses, so an error here means
// Run was called with an unvalidated Config.
func parseCIDRs(cidrs []string) ([]*net.IPNet, error) {
	out := make([]*net.IPNet, len(cidrs))
	for i, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			return nil, fmt.Errorf("app: http.trusted_proxies[%d]: %w", i, err)
		}
		out[i] = n
	}
	return out, nil
}

// parseLevel maps the configured log level (TECH-SPEC §1.1 logging) onto
// slog.Level. config.Validate already proved it is one of the four names;
// an unrecognised value falls back to Info rather than erroring — logging
// is diagnostic, not a start-up gate.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
